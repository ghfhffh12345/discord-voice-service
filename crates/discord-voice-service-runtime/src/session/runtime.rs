use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::Duration;

use discord_voice_service_playback::media::opus_queue::{
    OpusBufferDepth, OpusFrame, OpusFrameQueue,
};
use discord_voice_service_playback::media::position::SharedPlaybackPosition;
use discord_voice_service_playback::pacer::{AudioPacer, FRAME_DURATION};
use discord_voice_service_playback::recovery::PlaybackRecoveryMetrics;
use discord_voice_service_playback::source::PlaybackSource;
use discord_voice_service_playback::{PlaybackError, PlaybackWorker};
use discord_voice_service_voice::{ConnectedVoiceSession, VoiceContext, VoiceGatewayDrainReport};
use tokio::sync::{Mutex, Notify, RwLock, broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::events::{EventBus, SessionEventKind, SessionEventRecord};
use super::metrics::{
    MediaClockResetReason, PlaybackStabilityCollector, PlaybackStabilitySnapshot,
    ProducerMetricsSample,
};
use super::readiness::{
    ensure_active_voice_session, ensure_joinable_session, ensure_pauseable_track,
    ensure_resumable_track, ensure_track_loaded,
};
use super::state::{SessionState, Snapshot};
use super::supervisor::Command;
use crate::error::RuntimeError;

const PLAYBACK_QUEUE_CAPACITY: usize = 1024;
const PLAYBACK_BUFFER_MEMORY_CAP_BYTES: usize = 4 * 1024 * 1024;
const PLAYBACK_SOURCE_BUFFER_TARGET_MS: u64 = 5_000;
const PLAYBACK_SOURCE_BUFFER_HIGH_WATERMARK_MS: u64 = 5_000;
const PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS: u64 = 4_000;
const PLAYBACK_SOURCE_BUFFER_REFILL_BATCH_MS: u64 = 1_000;
const PLAYBACK_PIPELINE_METRICS_CHANNEL_CAPACITY: usize = 1024;
const PLAYBACK_MEDIA_COMMAND_CHANNEL_CAPACITY: usize = 32;
const MEDIA_SENDER_DAVE_READY_TIMEOUT: Duration = Duration::from_secs(5);
const MEDIA_SENDER_DAVE_READY_RETRY: Duration = Duration::from_millis(100);
const MEDIA_OWNER_RETURN_TIMEOUT: Duration = Duration::from_secs(5);

type LiveMediaDelayHook = Arc<dyn Fn(u64) -> Option<Duration> + Send + Sync>;

#[derive(Debug, Clone)]
struct PlaybackBufferPolicy {
    target_ms: u64,
}

impl PlaybackBufferPolicy {
    fn new() -> Self {
        Self {
            target_ms: PLAYBACK_SOURCE_BUFFER_TARGET_MS,
        }
    }

    fn target_ms(&self) -> u64 {
        self.target_ms
    }

    fn max_target_ms(&self) -> u64 {
        self.target_ms
    }

    fn record_refill(&mut self, duration: Duration) -> bool {
        let _ = duration;
        false
    }

    fn record_sender_lateness(&mut self, lateness: Duration) -> bool {
        let _ = lateness;
        false
    }
}

struct SharedSourceBuffer {
    state: Mutex<SourceBufferState>,
    changed: Notify,
}

impl SharedSourceBuffer {
    fn new(queue: OpusFrameQueue, end_of_stream: bool) -> Self {
        Self {
            state: Mutex::new(SourceBufferState {
                queue,
                end_of_stream,
                error: None,
            }),
            changed: Notify::new(),
        }
    }
}

struct SourceBufferState {
    queue: OpusFrameQueue,
    end_of_stream: bool,
    error: Option<PlaybackError>,
}

struct PlaybackSendInput {
    video_id: String,
    playback_epoch: u64,
    selected_itag: u32,
    voice_context: VoiceContext,
    voice_transport_generation: u64,
    session: ConnectedVoiceSession,
    playback: Arc<Mutex<PlaybackWorker>>,
    source: PlaybackSource,
    source_buffer: Arc<SharedSourceBuffer>,
    shared_position: SharedPlaybackPosition,
    position_ms: u64,
    initial_producer_sample: ProducerMetricsSample,
    recovery_metrics_baseline: PlaybackRecoveryMetrics,
    initial_speaking_prepare_duration: Duration,
}

struct LiveMediaDriverInput {
    playback_epoch: u64,
    current_playback_epoch: Arc<AtomicU64>,
    source_buffer: Arc<SharedSourceBuffer>,
    session: ConnectedVoiceSession,
    command_rx: mpsc::Receiver<MediaCommand>,
    metrics_tx: mpsc::Sender<PlaybackPipelineMetric>,
    live_media_delay_for_tests: Option<LiveMediaDelayHook>,
}

struct LiveMediaDriverHandle {
    result_rx: oneshot::Receiver<LiveMediaDriverExit>,
}

struct LiveMediaDriverExit {
    session: ConnectedVoiceSession,
    ended_naturally: bool,
    error: Option<RuntimeError>,
}

#[derive(Debug, Clone, Copy)]
enum MediaCommand {
    Pause,
    Resume,
    Stop,
}

struct LiveMediaDriver {
    session: ConnectedVoiceSession,
    playback_epoch: u64,
    current_playback_epoch: Arc<AtomicU64>,
    source_buffer: Arc<SharedSourceBuffer>,
    command_rx: mpsc::Receiver<MediaCommand>,
    metrics_tx: mpsc::Sender<PlaybackPipelineMetric>,
    live_media_delay_for_tests: Option<LiveMediaDelayHook>,
}

#[derive(Debug)]
struct SenderSentMetric {
    packet_index: u64,
    duration_ms: u64,
    is_track: bool,
    expected_deadline: Instant,
    send_started_at: Instant,
    sent_at: Instant,
    send_duration: Duration,
    non_send_work_duration: Duration,
    gateway_drain: VoiceGatewayDrainReport,
    media_clock_reset: bool,
    remaining_depth: OpusBufferDepth,
}

#[derive(Debug)]
enum PlaybackPipelineMetric {
    SourceProducer(ProducerMetricsSample),
    SourceDepth(OpusBufferDepth),
    SenderStarted { source_depth: OpusBufferDepth },
    SenderSent(SenderSentMetric),
    SenderSourceUnderrun { depth: OpusBufferDepth },
    SenderResumedAfterSourceUnderrun { depth: OpusBufferDepth },
    SenderResumedFromPause,
    SenderGatewayDrain(VoiceGatewayDrainReport),
    SenderMediaClockReset { reason: MediaClockResetReason },
    SenderStaleDaveSendPrevented,
}

pub struct VoiceSessionRuntime {
    state: RwLock<Snapshot>,
    events: EventBus,
    voice: Mutex<Option<ConnectedVoiceSession>>,
    media_commands: Mutex<Option<mpsc::Sender<MediaCommand>>>,
    media_owner_returned: Notify,
    media_owner_return_count: AtomicU64,
    playback: Option<Arc<Mutex<PlaybackWorker>>>,
    playback_metrics: RwLock<Option<PlaybackStabilitySnapshot>>,
    playback_epoch: Arc<AtomicU64>,
    rollover_epoch: Arc<AtomicU64>,
    voice_transport_generation: Arc<AtomicU64>,
    playback_gateway_interruptions: Arc<AtomicU64>,
    playback_dave_interruptions: Arc<AtomicU64>,
    playback_reconnect_interruptions: Arc<AtomicU64>,
    playback_reset_pending: AtomicBool,
    live_media_delay_for_tests: StdMutex<Option<LiveMediaDelayHook>>,
}

impl Default for VoiceSessionRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl VoiceSessionRuntime {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(Snapshot::default()),
            events: EventBus::new(64),
            voice: Mutex::new(None),
            media_commands: Mutex::new(None),
            media_owner_returned: Notify::new(),
            media_owner_return_count: AtomicU64::new(0),
            playback: None,
            playback_metrics: RwLock::new(None),
            playback_epoch: Arc::new(AtomicU64::new(0)),
            rollover_epoch: Arc::new(AtomicU64::new(0)),
            voice_transport_generation: Arc::new(AtomicU64::new(0)),
            playback_gateway_interruptions: Arc::new(AtomicU64::new(0)),
            playback_dave_interruptions: Arc::new(AtomicU64::new(0)),
            playback_reconnect_interruptions: Arc::new(AtomicU64::new(0)),
            playback_reset_pending: AtomicBool::new(false),
            live_media_delay_for_tests: StdMutex::new(None),
        }
    }

    pub fn with_playback_worker(worker: PlaybackWorker) -> Self {
        Self {
            state: RwLock::new(Snapshot::default()),
            events: EventBus::new(64),
            voice: Mutex::new(None),
            media_commands: Mutex::new(None),
            media_owner_returned: Notify::new(),
            media_owner_return_count: AtomicU64::new(0),
            playback: Some(Arc::new(Mutex::new(worker))),
            playback_metrics: RwLock::new(None),
            playback_epoch: Arc::new(AtomicU64::new(0)),
            rollover_epoch: Arc::new(AtomicU64::new(0)),
            voice_transport_generation: Arc::new(AtomicU64::new(0)),
            playback_gateway_interruptions: Arc::new(AtomicU64::new(0)),
            playback_dave_interruptions: Arc::new(AtomicU64::new(0)),
            playback_reconnect_interruptions: Arc::new(AtomicU64::new(0)),
            playback_reset_pending: AtomicBool::new(false),
            live_media_delay_for_tests: StdMutex::new(None),
        }
    }

    pub async fn handle_command(self: &Arc<Self>, command: Command) -> Result<(), RuntimeError> {
        match command {
            Command::JoinVoice { voice } => Box::pin(self.join_voice(voice)).await,
            Command::UpdateVoiceContext { voice } => {
                Box::pin(self.update_voice_context(voice)).await
            }
            Command::Play { video_id } => Box::pin(self.play(video_id)).await,
            Command::Pause => Box::pin(self.pause()).await,
            Command::Resume => Box::pin(self.resume()).await,
            Command::Stop => Box::pin(self.stop()).await,
            Command::LeaveVoice => Box::pin(self.leave_voice()).await,
        }
    }

    pub async fn snapshot(&self) -> Snapshot {
        self.state.read().await.clone()
    }

    pub async fn playback_metrics(&self) -> Option<PlaybackStabilitySnapshot> {
        self.playback_metrics.read().await.clone()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.events.subscribe()
    }

    #[cfg(debug_assertions)]
    pub fn set_live_media_send_delay_for_tests<F>(&self, delay_for_packet: F)
    where
        F: Fn(u64) -> Option<Duration> + Send + Sync + 'static,
    {
        *self
            .live_media_delay_for_tests
            .lock()
            .expect("live media test delay lock poisoned") = Some(Arc::new(delay_for_packet));
    }

    #[cfg(debug_assertions)]
    pub fn clear_live_media_send_delay_for_tests(&self) {
        *self
            .live_media_delay_for_tests
            .lock()
            .expect("live media test delay lock poisoned") = None;
    }

    async fn join_voice(&self, voice: VoiceContext) -> Result<(), RuntimeError> {
        let connecting_event = {
            let mut state = self.state.write().await;
            ensure_joinable_session(&state)?;
            apply_voice_context(&mut state, &voice);
            state.current_video_id = None;
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = 0;
            state.last_reason = None;
            state.state = SessionState::ConnectingVoice;
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceConnecting, &state)
        };
        self.events.emit(connecting_event);

        let mut session = ConnectedVoiceSession::connect(voice).await?;
        session.settle_initial_dave_for_join().await?;
        let ready_event = {
            let mut state = self.state.write().await;
            apply_voice_context(&mut state, session.voice_context());
            apply_rollover_state(&mut state, &session);
            state.current_video_id = None;
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = 0;
            state.last_reason = None;
            state.state = if session.is_connected() {
                SessionState::VoiceReady
            } else {
                SessionState::ConnectingVoice
            };

            *self.voice.lock().await = Some(session);
            if matches!(state.state, SessionState::VoiceReady) {
                Some(SessionEventRecord::from_snapshot(
                    SessionEventKind::VoiceReady,
                    &state,
                ))
            } else {
                None
            }
        };

        if let Some(event) = ready_event {
            self.events.emit(event);
        }
        Ok(())
    }

    async fn update_voice_context(
        self: &Arc<Self>,
        voice: VoiceContext,
    ) -> Result<(), RuntimeError> {
        let paused = {
            let state = self.state.read().await;
            ensure_active_voice_session(&state, "update_voice_context")?;
            matches!(state.state, SessionState::Paused)
        };
        if paused {
            return self.refresh_paused_voice_context(voice).await;
        }

        self.rollover_voice_context(voice).await
    }

    async fn play(self: &Arc<Self>, video_id: String) -> Result<(), RuntimeError> {
        let playback_epoch = self.begin_playback();
        self.play_with_epoch(video_id, playback_epoch).await
    }

    async fn play_with_epoch(
        self: &Arc<Self>,
        video_id: String,
        playback_epoch: u64,
    ) -> Result<(), RuntimeError> {
        let resume_position_hint = {
            let state = self.state.read().await;
            if state.current_video_id.as_deref() == Some(video_id.as_str()) {
                state.position_ms
            } else {
                0
            }
        };
        let resolving_event = {
            let mut state = self.state.write().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            ensure_active_voice_session(&state, "play")?;
            state.current_video_id = Some(video_id.clone());
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = resume_position_hint;
            state.last_reason = None;
            state.state = SessionState::ResolvingTrack;
            SessionEventRecord::from_snapshot(SessionEventKind::TrackResolving, &state)
        };
        tracing::debug!(%video_id, playback_epoch, "runtime emitting TrackResolving");
        self.events.emit(resolving_event);

        let Some(playback) = self.playback.as_ref().cloned() else {
            return Ok(());
        };

        if !self.wait_for_connected_voice(playback_epoch).await {
            return Ok(());
        }

        let mut source_queue = OpusFrameQueue::with_resource_limits(
            PLAYBACK_QUEUE_CAPACITY,
            PLAYBACK_BUFFER_MEMORY_CAP_BYTES,
            PLAYBACK_SOURCE_BUFFER_HIGH_WATERMARK_MS,
        );
        let prepare_started = Instant::now();
        let (
            selected_itag,
            mut source,
            resume_position_ms,
            shared_position,
            recovery_metrics_baseline,
        ) = {
            let mut worker = playback.lock().await;
            let recovery_metrics_baseline = worker.recovery_metrics();
            if self.consume_playback_reset() {
                worker.reset();
            }
            let source = worker.prepare(&video_id, &mut source_queue).await?;
            let selected_itag = source.selected_itag();
            let resume_position_ms = source.position().sent_duration_ms();
            let shared_position = source.shared_position();
            (
                selected_itag,
                source,
                resume_position_ms,
                shared_position,
                recovery_metrics_baseline,
            )
        };
        if self.playback_interrupted(playback_epoch) {
            return Ok(());
        }

        self.emit_playback_state(
            SessionEventKind::Buffering,
            &video_id,
            playback_epoch,
            selected_itag,
            source_queue.len(),
            resume_position_ms,
        )
        .await?;

        let (initial_recovery_metrics, source_ended_before_playing) = {
            let mut worker = playback.lock().await;
            worker
                .fill_queue_to_duration_ms(
                    &mut source,
                    &mut source_queue,
                    PLAYBACK_SOURCE_BUFFER_TARGET_MS,
                )
                .await?;
            let source_ended_before_playing =
                source_queue.buffered_duration_ms() < PLAYBACK_SOURCE_BUFFER_TARGET_MS;
            (worker.recovery_metrics(), source_ended_before_playing)
        };
        let initial_producer_sample = ProducerMetricsSample {
            fill_duration: prepare_started.elapsed(),
            produced_frames: source_queue.len(),
            source_buffer_depth: source_queue.depth(),
            stream_metrics: source.stream_metrics(),
            recovery_metrics: initial_recovery_metrics,
        };
        if self.playback_interrupted(playback_epoch) {
            return Ok(());
        }

        let (mut session, voice_context) = {
            let mut voice = self.voice.lock().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            let session = voice.as_mut().ok_or(RuntimeError::InvalidState(
                "play requires active voice session",
            ))?;
            tracing::debug!(
                %video_id,
                playback_epoch,
                "runtime handing complete voice session to live media driver"
            );
            let voice_context = session.voice_context().clone();
            let session = voice.take().ok_or(RuntimeError::InvalidState(
                "play requires owned voice session",
            ))?;
            (session, voice_context)
        };
        let initial_speaking_prepare_duration = match async {
            session.wait_for_initial_dave_settle().await?;
            session.prepare_speaking_before_media().await
        }
        .await
        {
            Ok(duration) => duration,
            Err(err) => {
                let mut voice = self.voice.lock().await;
                if voice.is_none() {
                    *voice = Some(session);
                }
                return Err(RuntimeError::from(err));
            }
        };
        if self.playback_interrupted(playback_epoch) {
            let mut voice = self.voice.lock().await;
            if voice.is_none() {
                *voice = Some(session);
            }
            return Ok(());
        }

        let source_buffer = Arc::new(SharedSourceBuffer::new(
            source_queue,
            source_ended_before_playing,
        ));

        self.send_playback_frames(PlaybackSendInput {
            video_id,
            playback_epoch,
            selected_itag,
            voice_context,
            voice_transport_generation: self.voice_transport_generation.load(Ordering::SeqCst),
            session,
            playback,
            source,
            source_buffer,
            shared_position,
            position_ms: resume_position_ms,
            initial_producer_sample,
            recovery_metrics_baseline,
            initial_speaking_prepare_duration,
        })
        .await
    }

    async fn emit_playback_state(
        &self,
        kind: SessionEventKind,
        video_id: &str,
        playback_epoch: u64,
        selected_itag: u32,
        queue_depth: usize,
        position_ms: u64,
    ) -> Result<(), RuntimeError> {
        let event = {
            let mut state = self.state.write().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            state.current_video_id = Some(video_id.to_owned());
            state.selected_itag = Some(selected_itag);
            state.queue_depth = queue_depth;
            state.position_ms = position_ms;
            state.state = match kind {
                SessionEventKind::Buffering => SessionState::Buffering,
                SessionEventKind::Playing => SessionState::Playing,
                _ => {
                    return Err(RuntimeError::InvalidState(
                        "unsupported playback state event",
                    ));
                }
            };
            SessionEventRecord::from_snapshot(kind.clone(), &state)
        };
        tracing::debug!(
            %video_id,
            playback_epoch,
            selected_itag,
            queue_depth,
            position_ms,
            event_kind = ?kind,
            "runtime emitting playback state"
        );
        self.events.emit(event);
        Ok(())
    }

    async fn publish_playback_metrics(&self, snapshot: PlaybackStabilitySnapshot) {
        tracing::event!(
            target: "discord_voice_service.playback.stability",
            tracing::Level::INFO,
            playback_epoch = snapshot.playback_epoch,
            video_id = snapshot.video_id.as_deref().unwrap_or(""),
            selected_itag = snapshot.selected_itag.unwrap_or_default(),
            track_packet_count = snapshot.track_packet_count,
            rtp_interval_p50_ms = snapshot.track_interval.p50_ms,
            rtp_interval_p95_ms = snapshot.track_interval.p95_ms,
            rtp_interval_p99_ms = snapshot.track_interval.p99_ms,
            rtp_interval_min_ms = snapshot.track_interval.min_ms,
            rtp_interval_max_ms = snapshot.track_interval.max_ms,
            sender_lateness_p50_ms = snapshot.sender_lateness.p50_ms,
            sender_lateness_p95_ms = snapshot.sender_lateness.p95_ms,
            sender_lateness_p99_ms = snapshot.sender_lateness.p99_ms,
            sender_lateness_max_ms = snapshot.sender_lateness.max_ms,
            max_consecutive_late_packets = snapshot.max_consecutive_late_packets,
            buffer_depth_ms = snapshot.current_buffer_depth.duration_ms,
            buffer_depth_packets = snapshot.current_buffer_depth.packets,
            buffer_depth_bytes = snapshot.current_buffer_depth.bytes,
            buffer_depth_samples = snapshot.current_buffer_depth.duration_samples,
            min_buffer_depth_ms = snapshot.min_buffer_depth.duration_ms,
            max_buffer_depth_ms = snapshot.max_buffer_depth.duration_ms,
            playout_buffer_depth_ms = snapshot.current_playout_buffer_depth.duration_ms,
            playout_buffer_depth_packets = snapshot.current_playout_buffer_depth.packets,
            min_playout_buffer_depth_ms = snapshot.min_playout_buffer_depth.duration_ms,
            max_playout_buffer_depth_ms = snapshot.max_playout_buffer_depth.duration_ms,
            prepared_rtp_queue_depth_ms = snapshot.prepared_rtp_queue_depth_ms,
            playout_buffer_low_watermark_count = snapshot.playout_buffer_low_watermark_count,
            playout_underrun_count = snapshot.playout_underrun_count,
            playout_sender_lateness_p50_ms = snapshot.playout_sender_lateness.p50_ms,
            playout_sender_lateness_p95_ms = snapshot.playout_sender_lateness.p95_ms,
            playout_sender_lateness_p99_ms = snapshot.playout_sender_lateness.p99_ms,
            playout_sender_lateness_max_ms = snapshot.playout_sender_lateness.max_ms,
            max_consecutive_playout_late_packets =
                snapshot.max_consecutive_playout_late_packets,
            speaking_prepare_p95_ms = snapshot.speaking_prepare_duration.p95_ms,
            speaking_prepare_max_ms = snapshot.speaking_prepare_duration.max_ms,
            adaptive_buffer_target_ms = snapshot.adaptive_buffer_target_ms,
            max_adaptive_buffer_target_ms = snapshot.max_adaptive_buffer_target_ms,
            buffer_low_watermark_count = snapshot.buffer_low_watermark_count,
            buffer_underrun_count = snapshot.buffer_underrun_count,
            refill_p95_ms = snapshot.refill_duration.p95_ms,
            refill_p99_ms = snapshot.refill_duration.p99_ms,
            refill_max_ms = snapshot.refill_duration.max_ms,
            producer_stall_p99_ms = snapshot.producer_stall_duration.p99_ms,
            max_producer_lag_ms = snapshot.max_producer_lag_ms,
            http_retry_count = snapshot.http_retry_count,
            response_open_count = snapshot.response_open_count,
            range_reopen_count = snapshot.range_reopen_count,
            read_error_reopen_count = snapshot.read_error_reopen_count,
            url_reresolve_count = snapshot.url_reresolve_count,
            pause_resume_first_intervals_ms = ?snapshot.pause_resume_first_intervals_ms,
            post_stall_first_intervals_ms = ?snapshot.post_stall_first_intervals_ms,
            inserted_silence_packet_count = snapshot.continuity_silence_packet_count,
            inserted_silence_duration_ms = snapshot.inserted_silence_duration_ms,
            gateway_interruptions = snapshot.gateway_interruptions,
            dave_interruptions = snapshot.dave_interruptions,
            reconnect_interruptions = snapshot.reconnect_interruptions,
            ended = snapshot.ended,
            "playback stability metrics"
        );
        *self.playback_metrics.write().await = Some(snapshot);
    }

    async fn send_media_command(&self, command: MediaCommand) {
        let sender = self.media_commands.lock().await.as_ref().cloned();
        if let Some(sender) = sender {
            let _ = sender.send(command).await;
        }
    }

    async fn install_media_commands(&self, sender: mpsc::Sender<MediaCommand>) {
        *self.media_commands.lock().await = Some(sender);
    }

    async fn clear_media_commands_if_current(&self, sender: &mpsc::Sender<MediaCommand>) {
        let mut guard = self.media_commands.lock().await;
        if guard
            .as_ref()
            .is_some_and(|active| active.same_channel(sender))
        {
            *guard = None;
        }
    }

    async fn clear_media_commands(&self) {
        *self.media_commands.lock().await = None;
    }

    async fn media_owner_is_active(&self) -> bool {
        self.media_commands.lock().await.is_some()
    }

    fn media_owner_return_count(&self) -> u64 {
        self.media_owner_return_count.load(Ordering::SeqCst)
    }

    fn mark_media_owner_returned(&self) {
        self.media_owner_return_count.fetch_add(1, Ordering::SeqCst);
        self.media_owner_returned.notify_waiters();
    }

    async fn wait_for_media_owner_session(
        &self,
        voice_transport_generation: u64,
    ) -> Result<(), RuntimeError> {
        let deadline = Instant::now() + MEDIA_OWNER_RETURN_TIMEOUT;
        loop {
            if self.voice_transport_generation.load(Ordering::SeqCst) != voice_transport_generation
            {
                return Ok(());
            }
            if self.voice.lock().await.is_some() {
                return Ok(());
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(RuntimeError::InvalidState(
                    "live media driver did not return voice session",
                ));
            }

            tokio::select! {
                () = self.media_owner_returned.notified() => {}
                () = tokio::time::sleep(
                    Duration::from_millis(20).min(deadline.saturating_duration_since(now)),
                ) => {}
            }
        }
    }

    async fn wait_for_media_owner_stopped_after(
        &self,
        previous_return_count: u64,
    ) -> Result<(), RuntimeError> {
        let deadline = Instant::now() + MEDIA_OWNER_RETURN_TIMEOUT;
        loop {
            if self.media_owner_return_count() > previous_return_count {
                return Ok(());
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(RuntimeError::InvalidState("live media driver did not stop"));
            }

            tokio::select! {
                () = self.media_owner_returned.notified() => {}
                () = tokio::time::sleep(
                    Duration::from_millis(20).min(deadline.saturating_duration_since(now)),
                ) => {}
            }
        }
    }

    async fn wait_for_connected_voice(&self, playback_epoch: u64) -> bool {
        let deadline = Instant::now() + MEDIA_SENDER_DAVE_READY_TIMEOUT;
        loop {
            if self.playback_interrupted(playback_epoch) {
                return false;
            }
            match self
                .voice
                .lock()
                .await
                .as_ref()
                .map(ConnectedVoiceSession::is_connected)
            {
                Some(true) => return true,
                Some(false) if Instant::now() < deadline => {
                    tokio::time::sleep(MEDIA_SENDER_DAVE_READY_RETRY).await;
                }
                _ => return false,
            }
        }
    }

    async fn send_playback_frames(&self, input: PlaybackSendInput) -> Result<(), RuntimeError> {
        let PlaybackSendInput {
            video_id,
            playback_epoch,
            selected_itag,
            voice_context: _voice_context,
            voice_transport_generation,
            session,
            playback,
            source,
            source_buffer,
            shared_position,
            position_ms: initial_position_ms,
            initial_producer_sample,
            recovery_metrics_baseline,
            initial_speaking_prepare_duration,
        } = input;
        let gateway_interruptions_baseline =
            self.playback_gateway_interruptions.load(Ordering::SeqCst);
        let dave_interruptions_baseline = self.playback_dave_interruptions.load(Ordering::SeqCst);
        let reconnect_interruptions_baseline =
            self.playback_reconnect_interruptions.load(Ordering::SeqCst);

        let mut metrics = PlaybackStabilityCollector::new(
            playback_epoch,
            video_id.clone(),
            selected_itag,
            OpusBufferDepth::default(),
            initial_producer_sample.source_buffer_depth,
            PLAYBACK_SOURCE_BUFFER_TARGET_MS,
            recovery_metrics_baseline,
        );
        let mut buffer_policy = PlaybackBufferPolicy::new();
        metrics.record_speaking_prepare_duration(initial_speaking_prepare_duration);
        metrics.record_playout_buffer_depth(OpusBufferDepth::default(), u64::MAX);
        let (buffer_target_tx, buffer_target_rx) = watch::channel(PLAYBACK_SOURCE_BUFFER_TARGET_MS);
        record_producer_sample_for_playback(
            &mut metrics,
            &mut buffer_policy,
            &buffer_target_tx,
            initial_producer_sample,
        );

        let (metrics_tx, mut metrics_rx) =
            mpsc::channel(PLAYBACK_PIPELINE_METRICS_CHANNEL_CAPACITY);
        let producer = spawn_playback_producer(
            playback,
            source,
            Arc::clone(&source_buffer),
            metrics_tx.clone(),
            buffer_target_rx,
        );

        let (command_tx, command_rx) = mpsc::channel(PLAYBACK_MEDIA_COMMAND_CHANNEL_CAPACITY);
        self.install_media_commands(command_tx.clone()).await;

        let mut latest_source_depth = source_buffer.state.lock().await.queue.depth();
        metrics.record_source_buffer_depth(
            latest_source_depth,
            PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS,
        );
        let mut position_ms = initial_position_ms;
        let mut buffering_for_source = false;
        let mut driver_done = false;
        let mut ended_naturally = false;
        let mut pipeline_result = Ok(());
        let mut returned_session = None;

        if latest_source_depth.packets > 0 {
            self.emit_playback_state(
                SessionEventKind::Playing,
                &video_id,
                playback_epoch,
                selected_itag,
                latest_source_depth.packets,
                position_ms,
            )
            .await?;
        }

        let mut driver_result_rx = spawn_live_media_driver(LiveMediaDriverInput {
            playback_epoch,
            current_playback_epoch: Arc::clone(&self.playback_epoch),
            source_buffer: Arc::clone(&source_buffer),
            session,
            command_rx,
            metrics_tx,
            live_media_delay_for_tests: self
                .live_media_delay_for_tests
                .lock()
                .expect("live media test delay lock poisoned")
                .clone(),
        })
        .result_rx;

        loop {
            if self.playback_interrupted(playback_epoch) {
                source_buffer.changed.notify_waiters();
                let _ = command_tx.try_send(MediaCommand::Stop);
            }

            tokio::select! {
                maybe_metric = metrics_rx.recv() => {
                    let Some(metric) = maybe_metric else {
                        break;
                    };
                    match metric {
                        PlaybackPipelineMetric::SourceProducer(sample) => {
                            record_producer_sample_for_playback(
                                &mut metrics,
                                &mut buffer_policy,
                                &buffer_target_tx,
                                sample,
                            );
                        }
                        PlaybackPipelineMetric::SourceDepth(depth) => {
                            metrics.record_source_buffer_depth(
                                depth,
                                PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS,
                            );
                        }
                        PlaybackPipelineMetric::SenderSourceUnderrun { depth } => {
                            latest_source_depth = depth;
                            metrics.record_source_underrun(depth);
                            if !buffering_for_source {
                                buffering_for_source = true;
                                self.emit_playback_state(
                                    SessionEventKind::Buffering,
                                    &video_id,
                                    playback_epoch,
                                    selected_itag,
                                    latest_source_depth.packets,
                                    position_ms,
                                )
                                .await?;
                            }
                        }
                        PlaybackPipelineMetric::SenderStarted { source_depth } => {
                            metrics.record_source_buffer_depth(
                                source_depth,
                                PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS,
                            );
                        }
                        PlaybackPipelineMetric::SenderSent(sent) => {
                            tracing::trace!(
                                playback_epoch,
                                packet_index = sent.packet_index,
                                "live media driver reported sent track packet"
                            );
                            latest_source_depth = sent.remaining_depth;
                            metrics.record_sender_send_duration(sent.send_duration);
                            metrics.record_sender_loop_non_send_work_duration(
                                sent.non_send_work_duration,
                            );
                            metrics.record_gateway_drain(sent.gateway_drain);
                            if sent.media_clock_reset {
                                metrics.record_media_clock_reset(
                                    MediaClockResetReason::SchedulerLate,
                                );
                            }
                            metrics.record_source_buffer_depth(
                                sent.remaining_depth,
                                PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS,
                            );
                            if sent.is_track {
                                metrics.record_track_packet(
                                    sent.expected_deadline,
                                    sent.send_started_at,
                                    sent.sent_at,
                                );
                                update_adaptive_buffer_target_from_lateness(
                                    &mut buffer_policy,
                                    &buffer_target_tx,
                                    &mut metrics,
                                    sent.send_started_at
                                        .checked_duration_since(sent.expected_deadline)
                                        .unwrap_or(Duration::ZERO),
                                );
                                shared_position
                                    .lock()
                                    .unwrap()
                                    .record_sent_packet(sent.duration_ms);
                                position_ms = position_ms.saturating_add(sent.duration_ms);
                            } else {
                                metrics.record_continuity_silence_packet(
                                    sent.sent_at,
                                    sent.duration_ms,
                                );
                            }

                            let mut state = self.state.write().await;
                            if !self.playback_interrupted(playback_epoch) {
                                state.queue_depth = latest_source_depth.packets;
                                state.position_ms = position_ms;
                            }
                        }
                        PlaybackPipelineMetric::SenderResumedAfterSourceUnderrun { depth } => {
                            latest_source_depth = depth;
                            metrics.record_source_buffer_depth(
                                depth,
                                PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS,
                            );
                            if buffering_for_source {
                                buffering_for_source = false;
                                self.emit_playback_state(
                                    SessionEventKind::Playing,
                                    &video_id,
                                    playback_epoch,
                                    selected_itag,
                                    latest_source_depth.packets,
                                    position_ms,
                                )
                                .await?;
                            }
                        }
                        PlaybackPipelineMetric::SenderResumedFromPause => {
                            metrics.record_resumed_from_pause();
                        }
                        PlaybackPipelineMetric::SenderGatewayDrain(report) => {
                            metrics.record_gateway_drain(report);
                        }
                        PlaybackPipelineMetric::SenderMediaClockReset { reason } => {
                            metrics.record_media_clock_reset(reason);
                        }
                        PlaybackPipelineMetric::SenderStaleDaveSendPrevented => {
                            metrics.record_stale_dave_send_prevented();
                        }
                    }
                }
                driver_result = &mut driver_result_rx, if !driver_done => {
                    driver_done = true;
                    match driver_result {
                        Ok(exit) => {
                            ended_naturally = exit.ended_naturally;
                            if let Some(err) = exit.error {
                                if let RuntimeError::Voice(voice_error) = &err {
                                    if voice_error.is_gateway_closed_during_receive() {
                                        self.playback_gateway_interruptions
                                            .fetch_add(1, Ordering::SeqCst);
                                    }
                                    if voice_error.to_string().contains("dave") {
                                        self.playback_dave_interruptions
                                            .fetch_add(1, Ordering::SeqCst);
                                    }
                                }
                                pipeline_result = Err(err);
                            }
                            returned_session = Some(exit.session);
                        }
                        Err(_) => {
                            pipeline_result = Err(RuntimeError::InvalidState(
                                "live media driver stopped without result",
                            ));
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }

            if driver_done || pipeline_result.is_err() {
                break;
            }
        }

        producer.abort();
        source_buffer.changed.notify_waiters();
        self.clear_media_commands_if_current(&command_tx).await;

        if !driver_done {
            let _ = command_tx.send(MediaCommand::Stop).await;
            match driver_result_rx.await {
                Ok(exit) => {
                    ended_naturally = ended_naturally || exit.ended_naturally;
                    if let Some(err) = exit.error
                        && pipeline_result.is_ok()
                    {
                        pipeline_result = Err(err);
                    }
                    returned_session = Some(exit.session);
                }
                Err(_) if pipeline_result.is_ok() => {
                    pipeline_result = Err(RuntimeError::InvalidState(
                        "live media driver stopped without result",
                    ));
                }
                _ => {}
            }
        }

        let interrupted = self.playback_interrupted(playback_epoch);
        let cleanup_result = if let Some(mut session) = returned_session {
            let restore_to_current_voice = self.voice_transport_generation.load(Ordering::SeqCst)
                == voice_transport_generation
                && self.voice.lock().await.is_none();
            let stop_result = if ended_naturally {
                session.stop_audio().await
            } else if interrupted {
                session.stop_speaking().await
            } else {
                Ok(())
            };
            if restore_to_current_voice {
                *self.voice.lock().await = Some(session);
            }
            self.mark_media_owner_returned();
            stop_result.map_err(RuntimeError::from)
        } else {
            self.mark_media_owner_returned();
            Ok(())
        };

        if ended_naturally && !interrupted && pipeline_result.is_ok() {
            self.defer_playback_reset();
            let track_ended_event = {
                let mut state = self.state.write().await;
                if self.playback_interrupted(playback_epoch) {
                    None
                } else {
                    state.position_ms = position_ms;
                    let event =
                        SessionEventRecord::from_snapshot(SessionEventKind::TrackEnded, &state);
                    state.current_video_id = None;
                    state.selected_itag = None;
                    state.queue_depth = 0;
                    state.position_ms = 0;
                    state.state = SessionState::VoiceReady;
                    Some(event)
                }
            };
            if let Some(event) = track_ended_event {
                self.events.emit(event);
            }
        }

        let snapshot = metrics.snapshot(
            self.playback_gateway_interruptions
                .load(Ordering::SeqCst)
                .saturating_sub(gateway_interruptions_baseline),
            self.playback_dave_interruptions
                .load(Ordering::SeqCst)
                .saturating_sub(dave_interruptions_baseline),
            self.playback_reconnect_interruptions
                .load(Ordering::SeqCst)
                .saturating_sub(reconnect_interruptions_baseline),
            ended_naturally && pipeline_result.is_ok() && !interrupted,
        );
        self.publish_playback_metrics(snapshot).await;
        match (pipeline_result, cleanup_result) {
            (Ok(()), Ok(_)) => Ok(()),
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
        }
    }

    async fn pause(&self) -> Result<(), RuntimeError> {
        {
            let state = self.state.read().await;
            ensure_track_loaded(&state, "pause")?;
            if matches!(state.state, SessionState::Paused) {
                tracing::debug!("runtime ignoring redundant Pause while already paused");
                return Ok(());
            }
            ensure_pauseable_track(&state)?;
        }
        self.send_media_command(MediaCommand::Pause).await;
        tracing::debug!("runtime pausing playback sender consumption");

        let event = {
            let mut state = self.state.write().await;
            ensure_track_loaded(&state, "pause")?;
            if matches!(state.state, SessionState::Paused) {
                tracing::debug!("runtime ignoring redundant Pause after state changed to paused");
                return Ok(());
            }
            ensure_pauseable_track(&state)?;
            state.state = SessionState::Paused;
            SessionEventRecord::from_snapshot(SessionEventKind::Paused, &state)
        };

        self.events.emit(event);
        Ok(())
    }

    async fn resume(&self) -> Result<(), RuntimeError> {
        {
            let state = self.state.read().await;
            ensure_track_loaded(&state, "resume")?;
            if matches!(state.state, SessionState::Playing) {
                tracing::debug!("runtime ignoring Resume while playback is already playing");
                return Ok(());
            }
            ensure_resumable_track(&state)?;
        }

        let event = {
            let mut state = self.state.write().await;
            ensure_track_loaded(&state, "resume")?;
            if matches!(state.state, SessionState::Playing) {
                tracing::debug!("runtime ignoring Resume after state changed to playing");
                return Ok(());
            }
            ensure_resumable_track(&state)?;
            state.state = SessionState::Playing;
            SessionEventRecord::from_snapshot(SessionEventKind::Playing, &state)
        };

        self.send_media_command(MediaCommand::Resume).await;
        self.events.emit(event);
        Ok(())
    }

    async fn stop(&self) -> Result<(), RuntimeError> {
        {
            let state = self.state.read().await;
            ensure_active_voice_session(&state, "stop")?;
        }
        let media_owner_was_active = self.media_owner_is_active().await;
        let voice_transport_generation = self.voice_transport_generation.load(Ordering::SeqCst);
        self.invalidate_playback();
        self.defer_playback_reset();
        self.send_media_command(MediaCommand::Stop).await;
        self.wait_for_media_owner_session(voice_transport_generation)
            .await?;
        let mut voice = self.voice.lock().await;
        if let Some(session) = voice.as_mut()
            && session.is_connected()
            && (media_owner_was_active || session.media_started())
        {
            session.stop_audio().await?;
        }

        let event = {
            let mut state = self.state.write().await;
            state.current_video_id = None;
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = 0;
            state.last_reason = None;
            state.state = SessionState::VoiceReady;
            SessionEventRecord::from_snapshot(SessionEventKind::Stopped, &state)
        };

        self.events.emit(event);
        Ok(())
    }

    async fn leave_voice(&self) -> Result<(), RuntimeError> {
        self.invalidate_rollover();
        self.invalidate_playback();
        self.defer_playback_reset();
        let media_owner_was_active = self.media_owner_is_active().await;
        let media_owner_return_count = self.media_owner_return_count();
        self.voice_transport_generation
            .fetch_add(1, Ordering::SeqCst);
        self.send_media_command(MediaCommand::Stop).await;
        if media_owner_was_active {
            self.wait_for_media_owner_stopped_after(media_owner_return_count)
                .await?;
        }
        self.clear_media_commands().await;
        *self.state.write().await = Snapshot::default();
        *self.voice.lock().await = None;
        Ok(())
    }

    async fn rollover_voice_context(
        self: &Arc<Self>,
        new_voice: VoiceContext,
    ) -> Result<(), RuntimeError> {
        let resume_video_id = {
            let state = self.state.read().await;
            ensure_active_voice_session(&state, "update_voice_context")?;
            state.current_video_id.clone()
        };
        let rollover_epoch = self.begin_rollover();

        if resume_video_id.is_some() {
            self.playback_reconnect_interruptions
                .fetch_add(1, Ordering::SeqCst);
        }
        if self.media_owner_is_active().await {
            let voice_transport_generation = self.voice_transport_generation.load(Ordering::SeqCst);
            self.invalidate_playback();
            self.send_media_command(MediaCommand::Stop).await;
            self.wait_for_media_owner_session(voice_transport_generation)
                .await?;
        }
        if self.current_voice_context_matches(&new_voice).await {
            return self
                .rollover_matching_voice_context(new_voice, resume_video_id, rollover_epoch)
                .await;
        }
        self.voice_transport_generation
            .fetch_add(1, Ordering::SeqCst);
        self.invalidate_playback();
        self.send_media_command(MediaCommand::Stop).await;
        self.clear_media_commands().await;
        let resume_playback_epoch = self.playback_epoch.load(Ordering::SeqCst);

        let reconnecting_event = {
            let mut state = self.state.write().await;
            state.voice_reconnecting = true;
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceReconnecting, &state)
        };
        self.events.emit(reconnecting_event);

        let mut replacement = match ConnectedVoiceSession::connect(new_voice.clone()).await {
            Ok(replacement) => replacement,
            Err(err) => {
                if !self.rollover_is_current(rollover_epoch) {
                    return Ok(());
                }
                if let Some(video_id) = resume_video_id.as_deref() {
                    if self
                        .rollover_resume_is_still_intended(
                            video_id,
                            resume_playback_epoch,
                            rollover_epoch,
                        )
                        .await
                    {
                        self.quiesce_current_transport().await;
                        self.interrupt_playback(format!("voice reconnect failed: {err}"))
                            .await;
                    } else {
                        self.recover_rollover_without_playback(format!(
                            "voice reconnect failed: {err}"
                        ))
                        .await;
                    }
                } else {
                    self.recover_rollover_without_playback(format!(
                        "voice reconnect failed: {err}"
                    ))
                    .await;
                }
                return Err(err.into());
            }
        };
        if let Err(err) = replacement.settle_initial_dave_for_join().await {
            if !self.rollover_is_current(rollover_epoch) {
                return Ok(());
            }
            if let Some(video_id) = resume_video_id.as_deref() {
                if self
                    .rollover_resume_is_still_intended(
                        video_id,
                        resume_playback_epoch,
                        rollover_epoch,
                    )
                    .await
                {
                    self.quiesce_current_transport().await;
                    self.interrupt_playback(format!("voice reconnect failed: {err}"))
                        .await;
                } else {
                    self.recover_rollover_without_playback(format!(
                        "voice reconnect failed: {err}"
                    ))
                    .await;
                }
            } else {
                self.recover_rollover_without_playback(format!("voice reconnect failed: {err}"))
                    .await;
            }
            return Err(err.into());
        }

        if !self.rollover_is_current(rollover_epoch) {
            return Ok(());
        }

        let reconnected_event = {
            let mut current_voice = self.voice.lock().await;
            *current_voice = Some(replacement);

            let (voice_context, rollover_recovering, rollover_reconnecting) = current_voice
                .as_ref()
                .map(|session| {
                    (
                        session.voice_context().clone(),
                        session.recovering(),
                        session.voice_reconnecting(),
                    )
                })
                .ok_or(RuntimeError::InvalidState(
                    "voice reconnect replacement missing",
                ))?;
            drop(current_voice);

            let mut state = self.state.write().await;
            apply_voice_context(&mut state, &voice_context);
            state.recovering = rollover_recovering;
            state.voice_reconnecting = rollover_reconnecting;
            state.last_reason = None;
            if state.current_video_id.is_none() {
                state.state = SessionState::VoiceReady;
            }
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceReady, &state)
        };
        self.events.emit(reconnected_event);

        if let Some(video_id) = resume_video_id.filter(|_| self.playback.is_some()) {
            if !self
                .rollover_resume_is_still_intended(
                    video_id.as_str(),
                    resume_playback_epoch,
                    rollover_epoch,
                )
                .await
            {
                return Ok(());
            }

            let runtime = Arc::clone(self);
            let resume_attempt_epoch = self.begin_playback();
            tokio::spawn(async move {
                runtime
                    .resume_after_rollover(video_id, resume_attempt_epoch, rollover_epoch)
                    .await;
            });
        }

        Ok(())
    }

    async fn current_voice_context_matches(&self, new_voice: &VoiceContext) -> bool {
        self.voice
            .lock()
            .await
            .as_ref()
            .is_some_and(|session| session.voice_context() == new_voice)
    }

    async fn rollover_matching_voice_context(
        self: &Arc<Self>,
        new_voice: VoiceContext,
        resume_video_id: Option<String>,
        rollover_epoch: u64,
    ) -> Result<(), RuntimeError> {
        self.invalidate_playback();
        let resume_playback_epoch = self.playback_epoch.load(Ordering::SeqCst);
        self.send_media_command(MediaCommand::Stop).await;

        let reconnecting_event = {
            let mut state = self.state.write().await;
            state.voice_reconnecting = true;
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceReconnecting, &state)
        };
        self.events.emit(reconnecting_event);

        let reconnected_event = {
            let mut current_voice = self.voice.lock().await;
            let session = current_voice.as_mut().ok_or(RuntimeError::InvalidState(
                "voice reconnect refresh missing active voice session",
            ))?;
            session.replace_voice_context(new_voice);
            let voice_context = session.voice_context().clone();
            let rollover_recovering = session.recovering();
            let rollover_reconnecting = session.voice_reconnecting();
            drop(current_voice);

            let mut state = self.state.write().await;
            apply_voice_context(&mut state, &voice_context);
            state.recovering = rollover_recovering;
            state.voice_reconnecting = rollover_reconnecting;
            state.last_reason = None;
            if state.current_video_id.is_none() {
                state.state = SessionState::VoiceReady;
            }
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceReady, &state)
        };
        self.events.emit(reconnected_event);

        if let Some(video_id) = resume_video_id.filter(|_| self.playback.is_some()) {
            if !self
                .rollover_resume_is_still_intended(
                    video_id.as_str(),
                    resume_playback_epoch,
                    rollover_epoch,
                )
                .await
            {
                return Ok(());
            }

            let runtime = Arc::clone(self);
            let resume_attempt_epoch = self.begin_playback();
            tokio::spawn(async move {
                runtime
                    .resume_after_rollover(video_id, resume_attempt_epoch, rollover_epoch)
                    .await;
            });
        }

        Ok(())
    }

    async fn refresh_paused_voice_context(
        &self,
        new_voice: VoiceContext,
    ) -> Result<(), RuntimeError> {
        tracing::debug!("runtime refreshing paused voice context");
        if self.media_commands.lock().await.is_some() {
            let mut state = self.state.write().await;
            if !matches!(state.state, SessionState::Paused) {
                return Ok(());
            }
            apply_voice_context(&mut state, &new_voice);
            state.recovering = false;
            state.voice_reconnecting = false;
            state.last_reason = None;
            tracing::debug!("runtime refreshed paused voice context while pipeline owns media");
            return Ok(());
        }

        {
            let mut current_voice = self.voice.lock().await;
            match current_voice.as_mut() {
                Some(session) => {
                    if session.is_connected() {
                        session.suspend_media().await?;
                    }
                    *current_voice = Some(ConnectedVoiceSession::disconnected(new_voice.clone()));
                }
                None => {
                    *current_voice = Some(ConnectedVoiceSession::disconnected(new_voice.clone()));
                }
            }
        }

        let mut state = self.state.write().await;
        if !matches!(state.state, SessionState::Paused) {
            return Ok(());
        }
        apply_voice_context(&mut state, &new_voice);
        state.recovering = false;
        state.voice_reconnecting = false;
        state.last_reason = None;
        tracing::debug!("runtime refreshed paused voice context");
        Ok(())
    }

    fn begin_playback(&self) -> u64 {
        let epoch = self.playback_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        epoch
    }

    fn invalidate_playback(&self) {
        self.playback_epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn begin_rollover(&self) -> u64 {
        self.rollover_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn invalidate_rollover(&self) {
        self.rollover_epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn defer_playback_reset(&self) {
        self.playback_reset_pending.store(true, Ordering::SeqCst);
    }

    fn consume_playback_reset(&self) -> bool {
        self.playback_reset_pending.swap(false, Ordering::SeqCst)
    }

    fn playback_interrupted(&self, playback_epoch: u64) -> bool {
        self.playback_epoch.load(Ordering::SeqCst) != playback_epoch
    }

    fn playback_state_is_current(&self, playback_epoch: u64) -> bool {
        self.playback_epoch.load(Ordering::SeqCst) == playback_epoch
    }

    fn rollover_is_current(&self, rollover_epoch: u64) -> bool {
        self.rollover_epoch.load(Ordering::SeqCst) == rollover_epoch
    }

    async fn rollover_resume_is_still_intended(
        &self,
        video_id: &str,
        resume_guard_epoch: u64,
        rollover_epoch: u64,
    ) -> bool {
        self.rollover_is_current(rollover_epoch)
            && self.playback_state_is_current(resume_guard_epoch)
            && self.state.read().await.current_video_id.as_deref() == Some(video_id)
    }

    async fn resume_attempt_is_still_current(
        &self,
        video_id: &str,
        resume_attempt_epoch: u64,
        rollover_epoch: u64,
    ) -> bool {
        self.rollover_is_current(rollover_epoch)
            && self.playback_state_is_current(resume_attempt_epoch)
            && self.state.read().await.current_video_id.as_deref() == Some(video_id)
    }

    async fn recover_rollover_without_playback(&self, reason: String) {
        let event = {
            let mut state = self.state.write().await;
            state.voice_reconnecting = false;
            state.last_reason = Some(reason);
            state.state = if state.guild_id.is_some() && state.channel_id.is_some() {
                SessionState::VoiceReady
            } else {
                SessionState::Idle
            };
            SessionEventRecord::from_snapshot(SessionEventKind::RecoverableWarning, &state)
        };
        self.events.emit(event);
    }

    async fn quiesce_current_transport(&self) {
        self.send_media_command(MediaCommand::Stop).await;
        let mut voice = self.voice.lock().await;
        if let Some(session) = voice.as_mut()
            && session.is_connected()
            && session.media_started()
        {
            let _ = session.stop_audio().await;
        }
    }

    async fn interrupt_playback(&self, reason: String) {
        let event = {
            let mut state = self.state.write().await;
            state.current_video_id = None;
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = 0;
            state.voice_reconnecting = false;
            state.last_reason = Some(reason);
            state.state = if state.guild_id.is_some() && state.channel_id.is_some() {
                SessionState::VoiceReady
            } else {
                SessionState::Idle
            };
            SessionEventRecord::from_snapshot(SessionEventKind::PlaybackInterrupted, &state)
        };
        self.events.emit(event);
    }

    async fn resume_after_rollover(
        self: Arc<Self>,
        video_id: String,
        resume_attempt_epoch: u64,
        rollover_epoch: u64,
    ) {
        if !self
            .resume_attempt_is_still_current(
                video_id.as_str(),
                resume_attempt_epoch,
                rollover_epoch,
            )
            .await
        {
            return;
        }

        if let Err(err) = self
            .play_with_epoch(video_id.clone(), resume_attempt_epoch)
            .await
        {
            let still_current = self
                .resume_attempt_is_still_current(
                    video_id.as_str(),
                    resume_attempt_epoch,
                    rollover_epoch,
                )
                .await;
            if still_current {
                self.quiesce_current_transport().await;
                self.interrupt_playback(format!(
                    "failed to resume playback after voice reconnect: {err}"
                ))
                .await;
            }
        }
    }
}

fn spawn_playback_producer(
    playback: Arc<Mutex<PlaybackWorker>>,
    mut source: PlaybackSource,
    source_buffer: Arc<SharedSourceBuffer>,
    metrics_tx: mpsc::Sender<PlaybackPipelineMetric>,
    buffer_target_rx: watch::Receiver<u64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let buffer_target_rx = buffer_target_rx;
        loop {
            let refill_target_ms = {
                let source_state = source_buffer.state.lock().await;
                if source_state.end_of_stream || source_state.error.is_some() {
                    return;
                }
                let target_ms =
                    (*buffer_target_rx.borrow()).clamp(1, PLAYBACK_SOURCE_BUFFER_HIGH_WATERMARK_MS);
                let buffered_ms = source_state.queue.buffered_duration_ms();
                if buffered_ms >= PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS {
                    None
                } else {
                    Some(
                        target_ms
                            .saturating_sub(buffered_ms)
                            .max(PLAYBACK_SOURCE_BUFFER_REFILL_BATCH_MS),
                    )
                }
            };

            let Some(refill_target_ms) = refill_target_ms else {
                source_buffer.changed.notified().await;
                continue;
            };

            let mut refill_queue = OpusFrameQueue::with_resource_limits(
                PLAYBACK_QUEUE_CAPACITY,
                PLAYBACK_BUFFER_MEMORY_CAP_BYTES,
                refill_target_ms,
            );
            let fill_started = Instant::now();
            let (fill_result, recovery_metrics) = {
                let mut worker = playback.lock().await;
                let fill_result = worker
                    .fill_queue_to_duration_ms(&mut source, &mut refill_queue, refill_target_ms)
                    .await;
                (fill_result, worker.recovery_metrics())
            };

            if let Err(err) = fill_result {
                let mut source_state = source_buffer.state.lock().await;
                source_state.error = Some(err);
                source_buffer.changed.notify_waiters();
                return;
            }

            let fill_duration = fill_started.elapsed();
            let produced_frames = refill_queue.len();
            let produced_duration_ms = refill_queue.buffered_duration_ms();
            let source_ended = produced_frames == 0 || produced_duration_ms < refill_target_ms;
            let source_buffer_depth = {
                let mut source_state = source_buffer.state.lock().await;
                while let Some(frame) = refill_queue.pop() {
                    if source_state.queue.push(frame).is_err() {
                        source_state.error = Some(PlaybackError::BufferFull);
                        source_buffer.changed.notify_waiters();
                        return;
                    }
                }
                if source_ended {
                    source_state.end_of_stream = true;
                }
                source_state.queue.depth()
            };
            source_buffer.changed.notify_waiters();

            let sample = ProducerMetricsSample {
                fill_duration,
                produced_frames,
                source_buffer_depth,
                stream_metrics: source.stream_metrics(),
                recovery_metrics,
            };
            let _ = metrics_tx.try_send(PlaybackPipelineMetric::SourceProducer(sample));

            if source_ended {
                return;
            }
        }
    })
}

fn spawn_live_media_driver(input: LiveMediaDriverInput) -> LiveMediaDriverHandle {
    let (result_tx, result_rx) = oneshot::channel();
    let _ = thread::Builder::new()
        .name(format!("discord-live-media-{}", input.playback_epoch))
        .spawn(move || {
            let driver = LiveMediaDriver::from(input);
            let exit = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(driver.run()),
                Err(_) => driver.into_exit(
                    false,
                    Some(RuntimeError::InvalidState(
                        "live media runtime failed to start",
                    )),
                ),
            };
            let _ = result_tx.send(exit);
        });
    LiveMediaDriverHandle { result_rx }
}

impl From<LiveMediaDriverInput> for LiveMediaDriver {
    fn from(input: LiveMediaDriverInput) -> Self {
        Self {
            session: input.session,
            playback_epoch: input.playback_epoch,
            current_playback_epoch: input.current_playback_epoch,
            source_buffer: input.source_buffer,
            command_rx: input.command_rx,
            metrics_tx: input.metrics_tx,
            live_media_delay_for_tests: input.live_media_delay_for_tests,
        }
    }
}

impl LiveMediaDriver {
    async fn run(mut self) -> LiveMediaDriverExit {
        match self.run_loop().await {
            Ok(ended_naturally) => self.into_exit(ended_naturally, None),
            Err(err) => self.into_exit(false, Some(err)),
        }
    }

    fn into_exit(self, ended_naturally: bool, error: Option<RuntimeError>) -> LiveMediaDriverExit {
        LiveMediaDriverExit {
            session: self.session,
            ended_naturally,
            error,
        }
    }

    async fn run_loop(&mut self) -> Result<bool, RuntimeError> {
        let mut pacer = AudioPacer::starting_after(FRAME_DURATION);
        let mut packet_index = 0u64;
        let mut paused = false;
        let mut source_underrun_active = false;
        let source_depth = current_source_depth(&self.source_buffer).await?;
        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::SenderStarted { source_depth });

        loop {
            if !self.playback_is_current() {
                return Ok(false);
            }
            if let Some(ended) = self.drain_commands(&mut paused, &mut pacer).await {
                return Ok(ended);
            }
            if paused {
                if let Some(ended) = self.wait_while_paused(&mut paused, &mut pacer).await {
                    return Ok(ended);
                }
                continue;
            }

            let expected_deadline = pacer.next_deadline();
            let non_send_work_started_at = Instant::now();
            let gateway_drain = match self
                .session
                .prepare_media_state_before_slot(expected_deadline)
                .await
            {
                Ok(report) => report,
                Err(err) if err.to_string().contains("dave") => {
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderMediaClockReset {
                                reason: MediaClockResetReason::DaveTransitionRecovery,
                            });
                    let _ = self
                        .metrics_tx
                        .try_send(PlaybackPipelineMetric::SenderStaleDaveSendPrevented);
                    pacer.reset_deadline();
                    let recovery_report = self
                        .session
                        .settle_pending_dave_transition_for_playback()
                        .await?;
                    let _ = self
                        .metrics_tx
                        .try_send(PlaybackPipelineMetric::SenderGatewayDrain(recovery_report));
                    pacer.reset_deadline();
                    continue;
                }
                Err(err) => return Err(RuntimeError::from(err)),
            };
            let source_poll =
                pop_live_source_frame(&self.source_buffer, self.playback_epoch, &self.metrics_tx)
                    .await?;
            let Some((frame, remaining_depth)) = source_poll.frame else {
                if source_poll.ended {
                    return Ok(true);
                }
                if !source_underrun_active {
                    source_underrun_active = true;
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderSourceUnderrun {
                                depth: source_poll.depth,
                            });
                }
                if let Some(ended) = self
                    .wait_for_source_or_control(
                        &mut paused,
                        &mut pacer,
                        &mut source_underrun_active,
                    )
                    .await?
                {
                    return Ok(ended);
                }
                continue;
            };

            if source_underrun_active {
                source_underrun_active = false;
                pacer.reset_deadline();
                let _ = self
                    .metrics_tx
                    .try_send(PlaybackPipelineMetric::SenderMediaClockReset {
                        reason: MediaClockResetReason::SourceUnderrun,
                    });
                let _ = self.metrics_tx.try_send(
                    PlaybackPipelineMetric::SenderResumedAfterSourceUnderrun {
                        depth: remaining_depth,
                    },
                );
                continue;
            }

            let frame_duration = frame_duration_from_samples(frame.duration_samples);
            self.session.prepare_speaking_before_media().await?;
            let packet = self
                .session
                .prepare_current_slot_audio_packet(frame.data, frame.duration_samples)?;
            let non_send_work_duration = non_send_work_started_at.elapsed();

            pacer.wait_until_ready().await;

            if !self.playback_is_current() {
                return Ok(false);
            }
            if let Some(delay) = self
                .live_media_delay_for_tests
                .as_ref()
                .and_then(|delay_for_packet| delay_for_packet(packet_index))
            {
                tokio::time::sleep(delay).await;
            }
            let send_started_at = Instant::now();
            self.session.send_current_slot_packet(packet).await?;
            let sent_at = Instant::now();
            let send_duration = sent_at.saturating_duration_since(send_started_at);
            let media_clock_reset = pacer.mark_sent(expected_deadline, frame_duration, sent_at);

            let _ =
                self.metrics_tx
                    .try_send(PlaybackPipelineMetric::SenderSent(SenderSentMetric {
                        packet_index,
                        duration_ms: frame.duration_ms,
                        is_track: true,
                        expected_deadline,
                        send_started_at,
                        sent_at,
                        send_duration,
                        non_send_work_duration,
                        gateway_drain,
                        media_clock_reset,
                        remaining_depth,
                    }));
            packet_index = packet_index.saturating_add(1);
        }
    }

    fn playback_is_current(&self) -> bool {
        self.current_playback_epoch.load(Ordering::SeqCst) == self.playback_epoch
    }

    async fn drain_commands(&mut self, paused: &mut bool, pacer: &mut AudioPacer) -> Option<bool> {
        while let Ok(command) = self.command_rx.try_recv() {
            if self.apply_command(command, paused, pacer).await {
                return Some(false);
            }
        }
        None
    }

    async fn wait_while_paused(
        &mut self,
        paused: &mut bool,
        pacer: &mut AudioPacer,
    ) -> Option<bool> {
        while *paused {
            if !self.playback_is_current() {
                return Some(false);
            }
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        return Some(false);
                    };
                    if self.apply_command(command, paused, pacer).await {
                        return Some(false);
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        None
    }

    async fn wait_for_source_or_control(
        &mut self,
        paused: &mut bool,
        pacer: &mut AudioPacer,
        source_underrun_active: &mut bool,
    ) -> Result<Option<bool>, RuntimeError> {
        loop {
            if !self.playback_is_current() {
                return Ok(Some(false));
            }
            if let Some(ended) = self.drain_commands(paused, pacer).await {
                return Ok(Some(ended));
            }
            if *paused {
                if let Some(ended) = self.wait_while_paused(paused, pacer).await {
                    return Ok(Some(ended));
                }
                continue;
            }

            let depth = current_source_depth(&self.source_buffer).await?;
            if depth.packets > 0 {
                if *source_underrun_active {
                    *source_underrun_active = false;
                    pacer.reset_deadline();
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderMediaClockReset {
                                reason: MediaClockResetReason::SourceUnderrun,
                            });
                    let _ = self.metrics_tx.try_send(
                        PlaybackPipelineMetric::SenderResumedAfterSourceUnderrun { depth },
                    );
                }
                return Ok(None);
            }

            tokio::select! {
                () = self.source_buffer.changed.notified() => {}
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        return Ok(Some(false));
                    };
                    if self.apply_command(command, paused, pacer).await {
                        return Ok(Some(false));
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
    }

    async fn apply_command(
        &mut self,
        command: MediaCommand,
        paused: &mut bool,
        pacer: &mut AudioPacer,
    ) -> bool {
        match command {
            MediaCommand::Pause => {
                *paused = true;
                pacer.reset_deadline();
                let _ = self
                    .metrics_tx
                    .try_send(PlaybackPipelineMetric::SenderMediaClockReset {
                        reason: MediaClockResetReason::PauseResume,
                    });
                false
            }
            MediaCommand::Resume => {
                if *paused {
                    *paused = false;
                    pacer.reset_deadline();
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderMediaClockReset {
                                reason: MediaClockResetReason::PauseResume,
                            });
                    let _ = self
                        .metrics_tx
                        .try_send(PlaybackPipelineMetric::SenderResumedFromPause);
                }
                false
            }
            MediaCommand::Stop => true,
        }
    }
}

#[derive(Debug)]
struct LiveSourcePoll {
    frame: Option<(OpusFrame, OpusBufferDepth)>,
    depth: OpusBufferDepth,
    ended: bool,
}

async fn pop_live_source_frame(
    source_buffer: &Arc<SharedSourceBuffer>,
    playback_epoch: u64,
    metrics_tx: &mpsc::Sender<PlaybackPipelineMetric>,
) -> Result<LiveSourcePoll, RuntimeError> {
    let (frame, depth, ended) = {
        let mut source_state = source_buffer.state.lock().await;
        if let Some(err) = source_state.error.take() {
            return Err(err.into());
        }
        let frame = source_state
            .queue
            .pop()
            .map(|frame| frame.with_epoch(playback_epoch));
        let depth = source_state.queue.depth();
        let ended = frame.is_none() && source_state.end_of_stream && depth.packets == 0;
        (frame, depth, ended)
    };
    source_buffer.changed.notify_waiters();
    let _ = metrics_tx.try_send(PlaybackPipelineMetric::SourceDepth(depth));

    Ok(LiveSourcePoll {
        frame: frame.map(|frame| (frame, depth)),
        depth,
        ended,
    })
}

async fn current_source_depth(
    source_buffer: &Arc<SharedSourceBuffer>,
) -> Result<OpusBufferDepth, RuntimeError> {
    let mut source_state = source_buffer.state.lock().await;
    if let Some(err) = source_state.error.take() {
        return Err(err.into());
    }
    Ok(source_state.queue.depth())
}

fn frame_duration_from_samples(duration_samples: u32) -> Duration {
    Duration::from_nanos(u64::from(duration_samples).saturating_mul(1_000_000_000) / 48_000)
}

fn record_producer_sample_for_playback(
    metrics: &mut PlaybackStabilityCollector,
    buffer_policy: &mut PlaybackBufferPolicy,
    buffer_target_tx: &watch::Sender<u64>,
    sample: ProducerMetricsSample,
) {
    metrics.record_producer_sample(sample);
    if buffer_policy.record_refill(sample.fill_duration) {
        buffer_target_tx.send_replace(buffer_policy.target_ms());
    }
    metrics.record_adaptive_buffer_target(buffer_policy.target_ms(), buffer_policy.max_target_ms());
}

fn update_adaptive_buffer_target_from_lateness(
    buffer_policy: &mut PlaybackBufferPolicy,
    buffer_target_tx: &watch::Sender<u64>,
    metrics: &mut PlaybackStabilityCollector,
    lateness: Duration,
) {
    if buffer_policy.record_sender_lateness(lateness) {
        buffer_target_tx.send_replace(buffer_policy.target_ms());
    }
    metrics.record_adaptive_buffer_target(buffer_policy.target_ms(), buffer_policy.max_target_ms());
}

fn apply_voice_context(snapshot: &mut Snapshot, voice: &VoiceContext) {
    snapshot.guild_id = Some(voice.guild_id.clone());
    snapshot.channel_id = Some(voice.channel_id.clone());
}

fn apply_rollover_state(snapshot: &mut Snapshot, session: &ConnectedVoiceSession) {
    snapshot.recovering = session.recovering();
    snapshot.voice_reconnecting = session.voice_reconnecting();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn playback_buffer_policy_reports_five_second_source_target() {
        let mut policy = PlaybackBufferPolicy::new();
        assert_eq!(policy.target_ms(), PLAYBACK_SOURCE_BUFFER_TARGET_MS);

        assert!(!policy.record_refill(Duration::from_millis(250)));
        assert_eq!(policy.target_ms(), PLAYBACK_SOURCE_BUFFER_TARGET_MS);

        assert!(!policy.record_sender_lateness(Duration::from_millis(40)));
        assert_eq!(policy.target_ms(), PLAYBACK_SOURCE_BUFFER_TARGET_MS);

        assert_eq!(policy.max_target_ms(), PLAYBACK_SOURCE_BUFFER_TARGET_MS);
    }

    #[test]
    fn playback_does_not_queue_prepared_rtp_packets() {
        let source = include_str!("runtime.rs");
        let forbidden = [
            ["VecDeque<", "PreparedVoicePacket>"].concat(),
            ["Sender<", "PreparedVoicePacket>"].concat(),
            ["Receiver<", "PreparedVoicePacket>"].concat(),
            ["Prepared", "PlayoutQueue"].concat(),
            ["spawn_", "playout_builder"].concat(),
            ["spawn_", "deadline_sender"].concat(),
        ];

        for forbidden_shape in forbidden {
            assert!(
                !source.contains(&forbidden_shape),
                "active runtime playback must not contain {forbidden_shape}"
            );
        }
    }

    #[test]
    fn playback_does_not_split_live_voice_state() {
        let source = include_str!("runtime.rs");
        let forbidden = [
            ["take_", "prepared_media_sender"].concat(),
            ["restore_", "prepared_media_sender"].concat(),
            ["Prepared", "MediaSender"].concat(),
            ["Prepared", "VoicePacketSender"].concat(),
        ];

        for forbidden_shape in forbidden {
            assert!(
                !source.contains(&forbidden_shape),
                "active runtime playback must not split live voice state through {forbidden_shape}"
            );
        }
    }

    #[test]
    fn live_driver_prepares_current_packet_before_send_boundary() {
        let source = include_str!("runtime.rs");
        assert!(
            !source.contains(".send_audio_frame_with_duration_samples(frame.data"),
            "live driver must not call the combined voice send path after the RTP boundary"
        );

        let prepare_state = source
            .find("prepare_media_state_before_slot(expected_deadline)")
            .expect("live loop should prepare media state before the slot");
        let prepare_packet = source
            .find("prepare_current_slot_audio_packet(frame.data")
            .expect("live loop should prepare one current-slot packet");
        let wait_boundary = source
            .find("pacer.wait_until_ready().await")
            .expect("live loop should sleep until the RTP send boundary");
        let hot_send = source
            .find("send_current_slot_packet(packet)")
            .expect("live loop should send the current-slot packet");

        assert!(prepare_state < wait_boundary);
        assert!(prepare_packet < wait_boundary);
        assert!(wait_boundary < hot_send);
    }

    #[tokio::test]
    async fn source_buffer_holds_raw_opus_frames_only() {
        let source_buffer = source_buffer_with_frames(250, false);
        let depth = current_source_depth(&source_buffer)
            .await
            .expect("source depth should be readable");

        assert_eq!(PLAYBACK_SOURCE_BUFFER_TARGET_MS, 5_000);
        assert_eq!(depth.packets, 250);
        assert_eq!(depth.duration_ms, PLAYBACK_SOURCE_BUFFER_TARGET_MS);
        assert_eq!(depth.duration_samples, 240_000);

        let (metrics_tx, _metrics_rx) = mpsc::channel(4);
        let poll = pop_live_source_frame(&source_buffer, 7, &metrics_tx)
            .await
            .expect("live source pop should succeed");
        let (frame, remaining_depth) = poll.frame.expect("source buffer should provide raw frame");
        assert_eq!(frame.duration_ms, 20);
        assert_eq!(frame.duration_samples, 960);
        assert_eq!(frame.data.len(), 8);
        assert_eq!(remaining_depth.duration_ms, 4_980);
    }

    #[tokio::test]
    async fn live_source_pop_reports_end_after_raw_reservoir_drains() {
        let source_buffer = source_buffer_with_frames(1, true);
        let (metrics_tx, mut metrics_rx) = mpsc::channel(8);

        let first = pop_live_source_frame(&source_buffer, 7, &metrics_tx)
            .await
            .expect("first source pop should succeed");
        assert!(first.frame.is_some());
        assert!(!first.ended);

        let second = pop_live_source_frame(&source_buffer, 7, &metrics_tx)
            .await
            .expect("second source pop should succeed");
        assert!(second.frame.is_none());
        assert!(second.ended);

        assert!(matches!(
            metrics_rx.try_recv().expect("first depth metric"),
            PlaybackPipelineMetric::SourceDepth(_)
        ));
        assert!(matches!(
            metrics_rx.try_recv().expect("second depth metric"),
            PlaybackPipelineMetric::SourceDepth(_)
        ));
    }

    #[test]
    fn source_buffer_metrics_report_five_second_target() {
        let source_depth = OpusBufferDepth {
            packets: 250,
            bytes: 128_000,
            duration_ms: PLAYBACK_SOURCE_BUFFER_TARGET_MS,
            duration_samples: 240_000,
        };
        let collector = PlaybackStabilityCollector::new(
            7,
            "video-1".into(),
            250,
            OpusBufferDepth::default(),
            source_depth,
            PLAYBACK_SOURCE_BUFFER_TARGET_MS,
            PlaybackRecoveryMetrics::default(),
        );

        let metrics = collector.snapshot(0, 0, 0, false);

        assert_eq!(
            metrics.source_buffer_target_ms,
            PLAYBACK_SOURCE_BUFFER_TARGET_MS
        );
        assert_eq!(metrics.current_source_buffer_depth.duration_ms, 5_000);
        assert_eq!(metrics.max_source_buffer_depth.duration_samples, 240_000);
        assert_eq!(metrics.current_playout_buffer_depth.duration_ms, 0);
        assert_eq!(metrics.max_playout_buffer_depth.duration_ms, 0);
    }

    #[tokio::test]
    async fn detached_rollover_resume_validates_the_claimed_epoch() {
        let runtime = Arc::new(VoiceSessionRuntime::new());
        {
            let mut state = runtime.state.write().await;
            state.current_video_id = Some("video-1".into());
        }

        let rollover_epoch = runtime.begin_rollover();
        runtime.invalidate_playback();
        let resume_guard_epoch = runtime.playback_epoch.load(Ordering::SeqCst);

        assert!(
            runtime
                .rollover_resume_is_still_intended("video-1", resume_guard_epoch, rollover_epoch,)
                .await
        );

        let resume_attempt_epoch = runtime.begin_playback();

        assert!(
            !runtime
                .rollover_resume_is_still_intended("video-1", resume_guard_epoch, rollover_epoch,)
                .await
        );
        assert!(
            runtime
                .resume_attempt_is_still_current("video-1", resume_attempt_epoch, rollover_epoch,)
                .await
        );
    }

    fn source_buffer_with_frames(count: usize, end_of_stream: bool) -> Arc<SharedSourceBuffer> {
        let mut queue = OpusFrameQueue::with_resource_limits(
            PLAYBACK_QUEUE_CAPACITY,
            PLAYBACK_BUFFER_MEMORY_CAP_BYTES,
            PLAYBACK_SOURCE_BUFFER_HIGH_WATERMARK_MS,
        );
        for index in 0..count {
            queue
                .push(OpusFrame::with_duration_samples(
                    vec![index as u8; 8].into(),
                    20,
                    960,
                ))
                .expect("test source buffer should accept frame");
        }
        Arc::new(SharedSourceBuffer::new(queue, end_of_stream))
    }
}
