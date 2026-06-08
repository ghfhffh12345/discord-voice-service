use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use discord_voice_service_playback::media::opus_queue::{
    OpusBufferDepth, OpusFrame, OpusFrameQueue,
};
use discord_voice_service_playback::media::position::SharedPlaybackPosition;
use discord_voice_service_playback::pacer::{
    AudioPacer, FRAME_DURATION, PacedPacketKind, SILENCE_FRAME,
};
use discord_voice_service_playback::recovery::PlaybackRecoveryMetrics;
use discord_voice_service_playback::source::PlaybackSource;
use discord_voice_service_playback::{PlaybackError, PlaybackWorker};
use discord_voice_service_voice::{
    ConnectedVoiceSession, VoiceContext, VoiceGatewayDrainReport, VoicePreparedPacketSender,
};
use tokio::sync::{Mutex, Notify, RwLock, broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::deadline_sender::{
    DeadlineDropRecord, DeadlineSendRecord, DeadlineSender, DeadlineSenderMetrics,
    PreparedMediaFrame, PreparedPacketKind, PreparedPlayoutCommand,
};
use super::events::{EventBus, SessionEventKind, SessionEventRecord};
use super::metrics::{
    MediaClockResetReason, PlaybackSendCommandKind, PlaybackSendEventRecord,
    PlaybackStabilityCollector, PlaybackStabilitySnapshot, PreparedPlayoutQueueEventKind,
    PreparedPlayoutQueueEventReason, PreparedPlayoutQueueEventSnapshot, ProducerMetricsSample,
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
const DISCORD_EGRESS_BUFFER_TARGET_MS: u64 = 400;
const DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS: u64 = 500;
const DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS: u64 = 300;
const STOP_AUDIO_BOUNDARY_SILENCE_PACKETS: usize = 5;
const PLAYBACK_PIPELINE_METRICS_CHANNEL_CAPACITY: usize = 1024;
const PLAYBACK_MEDIA_COMMAND_CHANNEL_CAPACITY: usize = 32;
const MEDIA_SENDER_DAVE_READY_TIMEOUT: Duration = Duration::from_secs(5);
const MEDIA_SENDER_DAVE_READY_RETRY: Duration = Duration::from_millis(100);
const MEDIA_OWNER_RETURN_TIMEOUT: Duration = Duration::from_secs(5);
const LIVE_DEADLINE_COMMAND_CHANNEL_CAPACITY: usize = 25;
const LIVE_DEADLINE_RECORD_CHANNEL_CAPACITY: usize = 64;
const DAVE_RECOVERY_DEADLINE_AHEAD_PACKETS: usize = 3;
const SOURCE_UNDERRUN_DEADLINE_AHEAD_PACKETS: usize = 3;
const DAVE_RECOVERY_DEADLINE_START_GUARD: Duration = FRAME_DURATION;
const DAVE_RECOVERY_GATEWAY_POLL: Duration = Duration::from_millis(1);

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

struct DiscordEgressBuffer {
    queue: OpusFrameQueue,
}

impl DiscordEgressBuffer {
    fn new() -> Self {
        Self {
            queue: OpusFrameQueue::with_resource_limits(
                PLAYBACK_QUEUE_CAPACITY,
                PLAYBACK_BUFFER_MEMORY_CAP_BYTES,
                DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS,
            ),
        }
    }

    fn push(&mut self, frame: OpusFrame) -> Result<(), OpusFrame> {
        self.queue.push(frame)
    }

    fn push_front(&mut self, frame: OpusFrame) -> Result<(), OpusFrame> {
        self.queue.push_front(frame)
    }

    fn pop(&mut self) -> Option<OpusFrame> {
        self.queue.pop()
    }

    fn depth(&self) -> OpusBufferDepth {
        self.queue.depth()
    }

    fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    fn buffered_duration_ms(&self) -> u64 {
        self.queue.buffered_duration_ms()
    }
}

struct PreparedTrackPlayout {
    command: PreparedPlayoutCommand,
    frame: OpusFrame,
}

struct PreparedPlayoutQueue {
    queue: VecDeque<PreparedTrackPlayout>,
    packets: usize,
    bytes: usize,
    duration_ms: u64,
    duration_samples: u64,
}

impl PreparedPlayoutQueue {
    fn new() -> Self {
        Self {
            queue: VecDeque::with_capacity(DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS as usize / 20),
            packets: 0,
            bytes: 0,
            duration_ms: 0,
            duration_samples: 0,
        }
    }

    fn push(&mut self, prepared: PreparedTrackPlayout) -> Result<(), PreparedTrackPlayout> {
        let frame = &prepared.frame;
        if self.duration_ms.saturating_add(frame.duration_ms)
            > DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS
        {
            return Err(prepared);
        }
        if self.packets >= PLAYBACK_QUEUE_CAPACITY {
            return Err(prepared);
        }
        if self.bytes.saturating_add(frame.byte_len()) > PLAYBACK_BUFFER_MEMORY_CAP_BYTES {
            return Err(prepared);
        }

        self.packets = self.packets.saturating_add(1);
        self.bytes = self.bytes.saturating_add(frame.byte_len());
        self.duration_ms = self.duration_ms.saturating_add(frame.duration_ms);
        self.duration_samples = self
            .duration_samples
            .saturating_add(u64::from(frame.duration_samples));
        self.queue.push_back(prepared);
        Ok(())
    }

    fn pop(&mut self) -> Option<PreparedTrackPlayout> {
        let prepared = self.queue.pop_front()?;
        self.packets = self.packets.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(prepared.frame.byte_len());
        self.duration_ms = self.duration_ms.saturating_sub(prepared.frame.duration_ms);
        self.duration_samples = self
            .duration_samples
            .saturating_sub(u64::from(prepared.frame.duration_samples));
        Some(prepared)
    }

    fn depth(&self) -> OpusBufferDepth {
        OpusBufferDepth {
            packets: self.packets,
            bytes: self.bytes,
            duration_ms: self.duration_ms,
            duration_samples: self.duration_samples,
        }
    }

    fn buffered_duration_ms(&self) -> u64 {
        self.duration_ms
    }

    fn is_full(&self) -> bool {
        self.duration_ms >= DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS
            || self.packets >= PLAYBACK_QUEUE_CAPACITY
            || self.bytes >= PLAYBACK_BUFFER_MEMORY_CAP_BYTES
    }
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
    Stop {
        account_popped_frame_as_skipped: bool,
    },
}

struct LiveMediaDriver {
    session: ConnectedVoiceSession,
    playback_epoch: u64,
    current_playback_epoch: Arc<AtomicU64>,
    source_buffer: Arc<SharedSourceBuffer>,
    command_rx: mpsc::Receiver<MediaCommand>,
    metrics_tx: mpsc::Sender<PlaybackPipelineMetric>,
    egress_buffer: DiscordEgressBuffer,
    prepared_playout_queue: PreparedPlayoutQueue,
    playout_generation: Arc<AtomicU64>,
    prepared_rebuild_credits: VecDeque<PreparedFrameRebuildCredit>,
    live_media_delay_for_tests: Option<LiveMediaDelayHook>,
    account_popped_frame_as_skipped_on_stop: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedFrameIdentity {
    epoch: u64,
    source_position_ms: u64,
    source_byte_position: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedFrameRebuildCredit {
    identity: PreparedFrameIdentity,
    reason: PreparedPlayoutQueueEventReason,
}

fn restore_interrupted_frames_to_egress_buffer(
    egress_buffer: &mut DiscordEgressBuffer,
    prepared_rebuild_credits: &mut VecDeque<PreparedFrameRebuildCredit>,
    reason: PreparedPlayoutQueueEventReason,
    frames: Vec<OpusFrame>,
) -> (u64, u64, Vec<OpusFrame>) {
    let mut restored_count = 0u64;
    let mut restored_duration_ms = 0u64;
    let mut source_restore_frames = Vec::new();
    for frame in frames {
        let restored_frame = frame.clone();
        match egress_buffer.push(frame) {
            Ok(()) => {
                restored_count = restored_count.saturating_add(1);
                restored_duration_ms =
                    restored_duration_ms.saturating_add(restored_frame.duration_ms);
                prepared_rebuild_credits.push_back(PreparedFrameRebuildCredit {
                    identity: prepared_frame_identity(&restored_frame),
                    reason,
                });
            }
            Err(frame) => source_restore_frames.push(frame),
        }
    }

    (restored_count, restored_duration_ms, source_restore_frames)
}

enum LiveDeadlineOutcome {
    Sent(DeadlineSendRecord),
    Dropped(DeadlineDropRecord),
}

#[derive(Debug)]
struct PendingDeadlineCommand {
    frame: Option<OpusFrame>,
    kind: PreparedPacketKind,
    duration_ms: u64,
    duration_samples: u32,
    rtp_sequence: u16,
    rtp_timestamp: u32,
    protection_nonce: Option<u32>,
    media_frame: Option<PreparedMediaFrame>,
    generation: u64,
}

impl PendingDeadlineCommand {
    fn new(command: &PreparedPlayoutCommand, frame: Option<OpusFrame>) -> Self {
        Self {
            frame,
            kind: command.kind,
            duration_ms: command.packet.duration_ms,
            duration_samples: command.packet.duration_samples,
            rtp_sequence: command.packet.rtp_sequence,
            rtp_timestamp: command.packet.rtp_timestamp,
            protection_nonce: command.packet.protection_nonce,
            media_frame: command.media_frame,
            generation: command.generation,
        }
    }

    fn track_depth(&self) -> OpusBufferDepth {
        let Some(frame) = &self.frame else {
            return OpusBufferDepth::default();
        };
        OpusBufferDepth {
            packets: 1,
            bytes: frame.byte_len(),
            duration_ms: frame.duration_ms,
            duration_samples: u64::from(frame.duration_samples),
        }
    }

    fn matches_sent_record(&self, record: &DeadlineSendRecord) -> bool {
        self.kind == record.kind
            && self.duration_ms == record.duration_ms
            && self.duration_samples == record.duration_samples
            && self.rtp_sequence == record.rtp_sequence
            && self.rtp_timestamp == record.rtp_timestamp
            && self.protection_nonce == record.protection_nonce
            && self.media_frame == record.media_frame
    }

    fn matches_drop_record(&self, record: &DeadlineDropRecord) -> bool {
        self.kind == record.kind
            && self.duration_ms == record.duration_ms
            && self.duration_samples == record.duration_samples
            && self.rtp_sequence == record.rtp_sequence
            && self.rtp_timestamp == record.rtp_timestamp
            && self.protection_nonce == record.protection_nonce
            && self.media_frame == record.media_frame
            && self.generation == record.generation
    }
}

struct LiveDeadlineSender {
    command_tx: mpsc::Sender<PreparedPlayoutCommand>,
    send_record_rx: mpsc::Receiver<DeadlineSendRecord>,
    drop_record_rx: mpsc::Receiver<DeadlineDropRecord>,
    shutdown: Arc<AtomicBool>,
    task: Option<JoinHandle<Result<DeadlineSenderMetrics, RuntimeError>>>,
}

impl LiveDeadlineSender {
    fn spawn(
        sink: VoicePreparedPacketSender,
        active_generation: Arc<AtomicU64>,
        next_deadline: Instant,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(LIVE_DEADLINE_COMMAND_CHANNEL_CAPACITY);
        let (send_record_tx, send_record_rx) = mpsc::channel(LIVE_DEADLINE_RECORD_CHANNEL_CAPACITY);
        let (drop_record_tx, drop_record_rx) = mpsc::channel(LIVE_DEADLINE_RECORD_CHANNEL_CAPACITY);
        let shutdown = Arc::new(AtomicBool::new(false));
        let sender = DeadlineSender::new_with_next_deadline_records_and_generation(
            sink,
            command_rx,
            send_record_tx,
            drop_record_tx,
            active_generation,
            Arc::clone(&shutdown),
            next_deadline,
        );
        let task = tokio::spawn(async move { sender.run().await });

        Self {
            command_tx,
            send_record_rx,
            drop_record_rx,
            shutdown,
            task: Some(task),
        }
    }

    async fn send_command(&self, command: PreparedPlayoutCommand) -> Result<(), RuntimeError> {
        self.command_tx
            .send(command)
            .await
            .map_err(|_| RuntimeError::InvalidState("deadline sender command channel closed"))
    }

    fn try_send_command(&self, command: PreparedPlayoutCommand) -> Result<(), RuntimeError> {
        self.command_tx.try_send(command).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => {
                RuntimeError::InvalidState("deadline sender command channel full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                RuntimeError::InvalidState("deadline sender command channel closed")
            }
        })
    }

    fn available_command_capacity(&self) -> usize {
        self.command_tx.capacity()
    }

    fn try_next_outcome(&mut self) -> Result<Option<LiveDeadlineOutcome>, RuntimeError> {
        match self.send_record_rx.try_recv() {
            Ok(record) => return Ok(Some(LiveDeadlineOutcome::Sent(record))),
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => {
                return Err(RuntimeError::InvalidState(
                    "deadline sender send record channel closed",
                ));
            }
        }
        match self.drop_record_rx.try_recv() {
            Ok(record) => Ok(Some(LiveDeadlineOutcome::Dropped(record))),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(RuntimeError::InvalidState(
                "deadline sender drop record channel closed",
            )),
        }
    }

    async fn next_outcome(&mut self) -> Result<LiveDeadlineOutcome, RuntimeError> {
        tokio::select! {
            record = self.send_record_rx.recv() => {
                record
                    .map(LiveDeadlineOutcome::Sent)
                    .ok_or(RuntimeError::InvalidState("deadline sender send record channel closed"))
            }
            record = self.drop_record_rx.recv() => {
                record
                    .map(LiveDeadlineOutcome::Dropped)
                    .ok_or(RuntimeError::InvalidState("deadline sender drop record channel closed"))
            }
        }
    }

    async fn shutdown(mut self) -> Result<(), RuntimeError> {
        self.shutdown.store(true, Ordering::Release);
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        match task.await {
            Ok(Ok(_metrics)) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(_join_error) => Err(RuntimeError::InvalidState("deadline sender task failed")),
        }
    }
}

impl Drop for LiveDeadlineSender {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug)]
struct SenderSentMetric {
    packet_index: u64,
    kind: PreparedPacketKind,
    duration_ms: u64,
    duration_samples: u32,
    is_track: bool,
    expected_deadline: Instant,
    send_started_at: Instant,
    sent_at: Instant,
    send_duration: Duration,
    rtp_sequence: u16,
    rtp_timestamp: u32,
    protection_nonce: Option<u32>,
    media_frame: Option<PreparedMediaFrame>,
    non_send_work_duration: Duration,
    gateway_drain: VoiceGatewayDrainReport,
    media_clock_reset: bool,
    tempo_rebased: bool,
    remaining_depth: OpusBufferDepth,
    egress_depth: OpusBufferDepth,
}

#[derive(Debug)]
enum PlaybackPipelineMetric {
    SourceProducer(ProducerMetricsSample),
    SourceDepth(OpusBufferDepth),
    SenderStarted {
        source_depth: OpusBufferDepth,
    },
    EgressDepth(OpusBufferDepth),
    PreparedTrackQueueDepth(OpusBufferDepth),
    NonTrackPlayoutQueueDepth {
        command_kind: PlaybackSendCommandKind,
        depth: OpusBufferDepth,
    },
    PreparedPlayoutQueueEvent(PreparedPlayoutQueueEventSnapshot),
    ExplicitMediaBoundary {
        reason: PreparedPlayoutQueueEventReason,
    },
    DaveTransitionReachedBuilder,
    SourceUnderrunReachedBuilder,
    SourceUnderrunReachedDeadlineSender,
    RestoredSourceFrames {
        frame_count: u64,
        duration_ms: u64,
    },
    DiscardedSourceFrames {
        reason: PreparedPlayoutQueueEventReason,
        frame_count: u64,
        duration_ms: u64,
    },
    PlayoutBuilderPrepared {
        duration: Duration,
    },
    SenderSent(SenderSentMetric),
    SenderEgressUnderrun {
        depth: OpusBufferDepth,
    },
    SenderSkippedSourceFrames {
        frame_count: u64,
        duration_ms: u64,
        remaining_depth: OpusBufferDepth,
    },
    EgressDroppedMusicFrames {
        frame_count: u64,
        duration_ms: u64,
    },
    SenderSourceUnderrun {
        depth: OpusBufferDepth,
    },
    SenderResumedAfterSourceUnderrun {
        depth: OpusBufferDepth,
    },
    SenderResumedFromPause,
    SenderMediaClockReset {
        reason: MediaClockResetReason,
    },
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
            track_media_duration_sent_ms = snapshot.track_media_duration_sent_ms,
            track_wall_clock_elapsed_ms = snapshot.track_wall_clock_elapsed_ms,
            track_media_to_wall_clock_ratio_ppm =
                snapshot.track_media_to_wall_clock_ratio_ppm,
            track_fast_interval_count = snapshot.track_fast_interval_count,
            track_fast_interval_min_ms = snapshot.track_fast_interval_min_ms,
            track_fast_interval_min_us = snapshot.track_fast_interval_min_us,
            track_tempo_window_count = snapshot.track_tempo_window_count,
            track_tempo_window_post_source_buffer_count =
                snapshot.track_tempo_window_post_source_buffer_count,
            track_tempo_window_min_ratio_ppm = snapshot.track_tempo_window_min_ratio_ppm,
            track_tempo_window_max_ratio_ppm = snapshot.track_tempo_window_max_ratio_ppm,
            track_tempo_window_fast_count = snapshot.track_tempo_window_fast_count,
            track_tempo_window_fastest_ratio_ppm =
                snapshot.track_tempo_window_fastest_ratio_ppm,
            track_tempo_window_fastest_media_ms =
                snapshot.track_tempo_window_fastest_media_ms,
            track_tempo_window_fastest_wall_clock_us =
                snapshot.track_tempo_window_fastest_wall_clock_us,
            track_tempo_window_slow_count = snapshot.track_tempo_window_slow_count,
            track_tempo_window_slowest_ratio_ppm =
                snapshot.track_tempo_window_slowest_ratio_ppm,
            track_tempo_window_slowest_media_ms =
                snapshot.track_tempo_window_slowest_media_ms,
            track_tempo_window_slowest_wall_clock_us =
                snapshot.track_tempo_window_slowest_wall_clock_us,
            skipped_source_frame_count = snapshot.skipped_source_frame_count,
            skipped_source_duration_ms = snapshot.skipped_source_duration_ms,
            tempo_rebase_count = snapshot.tempo_rebase_count,
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
            prepared_track_queue_target_ms = snapshot.prepared_track_queue_target_ms,
            prepared_track_queue_low_watermark_ms =
                snapshot.prepared_track_queue_low_watermark_ms,
            prepared_track_queue_high_watermark_ms =
                snapshot.prepared_track_queue_high_watermark_ms,
            active_pre_pause_prepared_track_queue_depth_min_ms =
                snapshot.active_pre_pause_prepared_track_queue_depth.min_depth.duration_ms,
            active_pre_pause_prepared_track_queue_depth_p5_ms =
                snapshot.active_pre_pause_prepared_track_queue_depth.p5_depth.duration_ms,
            active_pre_pause_prepared_track_queue_depth_p50_ms =
                snapshot.active_pre_pause_prepared_track_queue_depth.p50_depth.duration_ms,
            active_pre_pause_prepared_track_queue_depth_p95_ms =
                snapshot.active_pre_pause_prepared_track_queue_depth.p95_depth.duration_ms,
            active_pre_pause_prepared_track_queue_empty_count =
                snapshot.active_pre_pause_prepared_track_queue_depth.empty_count,
            active_post_resume_prepared_track_queue_depth_min_ms =
                snapshot.active_post_resume_prepared_track_queue_depth.min_depth.duration_ms,
            active_post_resume_prepared_track_queue_depth_p5_ms =
                snapshot.active_post_resume_prepared_track_queue_depth.p5_depth.duration_ms,
            active_post_resume_prepared_track_queue_depth_p50_ms =
                snapshot.active_post_resume_prepared_track_queue_depth.p50_depth.duration_ms,
            active_post_resume_prepared_track_queue_depth_p95_ms =
                snapshot.active_post_resume_prepared_track_queue_depth.p95_depth.duration_ms,
            active_post_resume_prepared_track_queue_empty_count =
                snapshot.active_post_resume_prepared_track_queue_depth.empty_count,
            prepared_track_queue_depth_sample_count =
                snapshot.prepared_track_queue_depth_sample_count,
            prepared_track_queue_empty_count = snapshot.prepared_track_queue_empty_count,
            prepared_track_packet_drop_count = snapshot.prepared_track_packet_drop_count,
            prepared_silence_packet_drop_count = snapshot.prepared_silence_packet_drop_count,
            prepared_packet_rebuild_count = snapshot.prepared_packet_rebuild_count,
            scheduled_silence_packet_count = snapshot.scheduled_silence_packet_count,
            pause_media_boundary_count = snapshot.pause_media_boundary_count,
            stop_media_boundary_count = snapshot.stop_media_boundary_count,
            recovery_media_boundary_count = snapshot.recovery_media_boundary_count,
            natural_end_media_boundary_count = snapshot.natural_end_media_boundary_count,
            restored_source_frame_count = snapshot.restored_source_frame_count,
            discarded_source_frame_count = snapshot.discarded_source_frame_count,
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
            DISCORD_EGRESS_BUFFER_TARGET_MS,
            DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS,
            DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS,
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
                let _ = command_tx.try_send(MediaCommand::Stop {
                    account_popped_frame_as_skipped: true,
                });
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
                        PlaybackPipelineMetric::EgressDepth(depth) => {
                            metrics.record_playout_buffer_depth(
                                depth,
                                DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS,
                            );
                        }
                        PlaybackPipelineMetric::PreparedTrackQueueDepth(depth) => {
                            metrics.record_prepared_track_queue_depth(depth);
                        }
                        PlaybackPipelineMetric::NonTrackPlayoutQueueDepth {
                            command_kind,
                            depth,
                        } => {
                            metrics.record_non_track_playout_queue_depth(command_kind, depth);
                        }
                        PlaybackPipelineMetric::PreparedPlayoutQueueEvent(event) => {
                            metrics.record_prepared_playout_queue_event(event);
                        }
                        PlaybackPipelineMetric::ExplicitMediaBoundary { reason } => {
                            metrics.record_explicit_media_boundary(reason);
                        }
                        PlaybackPipelineMetric::DaveTransitionReachedBuilder => {
                            metrics.record_dave_transition_reached_builder();
                        }
                        PlaybackPipelineMetric::SourceUnderrunReachedBuilder => {
                            metrics.record_source_underrun_reached_builder();
                        }
                        PlaybackPipelineMetric::SourceUnderrunReachedDeadlineSender => {
                            metrics.record_source_underrun_reached_deadline_sender();
                        }
                        PlaybackPipelineMetric::RestoredSourceFrames {
                            frame_count,
                            duration_ms,
                        } => {
                            metrics.record_restored_source_frames(frame_count, duration_ms);
                        }
                        PlaybackPipelineMetric::DiscardedSourceFrames {
                            reason,
                            frame_count,
                            duration_ms,
                        } => {
                            metrics.record_discarded_source_frames(
                                reason,
                                frame_count,
                                duration_ms,
                            );
                        }
                        PlaybackPipelineMetric::PlayoutBuilderPrepared { duration } => {
                            metrics.record_playout_builder_prepare_duration(duration);
                        }
                        PlaybackPipelineMetric::SenderEgressUnderrun { depth } => {
                            metrics.record_playout_underrun(depth);
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
                            metrics.record_send_event(PlaybackSendEventRecord {
                                packet_index: sent.packet_index,
                                command_kind: playback_send_command_kind(sent.kind),
                                expected_deadline: sent.expected_deadline,
                                send_started_at: sent.send_started_at,
                                sent_at: sent.sent_at,
                                media_duration_ms: sent.duration_ms,
                                media_duration_samples: sent.duration_samples,
                                rtp_sequence: sent.rtp_sequence,
                                rtp_timestamp: sent.rtp_timestamp,
                                protection_nonce: sent.protection_nonce,
                                source_frame_epoch: sent.media_frame.map(|frame| frame.epoch),
                                source_media_position_ms: sent
                                    .media_frame
                                    .map(|frame| frame.media_position_ms),
                                source_media_byte_position: sent
                                    .media_frame
                                    .and_then(|frame| frame.media_byte_position),
                                committed_heard_media: sent.media_frame.is_some(),
                            });
                            if sent.media_clock_reset {
                                metrics.record_media_clock_reset(
                                    MediaClockResetReason::SchedulerLate,
                                );
                            }
                            metrics.record_source_buffer_depth(
                                sent.remaining_depth,
                                PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS,
                            );
                            metrics.record_playout_buffer_depth(
                                sent.egress_depth,
                                DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS,
                            );
                            if sent.is_track {
                                metrics.record_track_packet(
                                    sent.expected_deadline,
                                    sent.send_started_at,
                                    sent.sent_at,
                                    sent.duration_ms,
                                    sent.tempo_rebased,
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
                            } else if matches!(sent.kind, PreparedPacketKind::ScheduledSilence) {
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
                        PlaybackPipelineMetric::SenderSkippedSourceFrames {
                            frame_count,
                            duration_ms,
                            remaining_depth,
                        } => {
                            latest_source_depth = remaining_depth;
                            metrics.record_skipped_source_frames(frame_count, duration_ms);
                            metrics.record_source_buffer_depth(
                                remaining_depth,
                                PLAYBACK_SOURCE_BUFFER_LOW_WATERMARK_MS,
                            );
                            let mut state = self.state.write().await;
                            if !self.playback_interrupted(playback_epoch) {
                                state.queue_depth = latest_source_depth.packets;
                                state.position_ms = position_ms;
                            }
                        }
                        PlaybackPipelineMetric::EgressDroppedMusicFrames {
                            frame_count,
                            duration_ms,
                        } => {
                            metrics.record_egress_dropped_music_frames(frame_count, duration_ms);
                        }
                        PlaybackPipelineMetric::SenderResumedAfterSourceUnderrun { depth } => {
                            latest_source_depth = depth;
                            metrics.record_rebuffer(
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
            let _ = command_tx
                .send(MediaCommand::Stop {
                    account_popped_frame_as_skipped: true,
                })
                .await;
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
        let mut cleanup_boundary_reason = None;
        let cleanup_result = if let Some(mut session) = returned_session {
            let restore_to_current_voice = self.voice_transport_generation.load(Ordering::SeqCst)
                == voice_transport_generation
                && self.voice.lock().await.is_none();
            let stop_result = if ended_naturally {
                cleanup_boundary_reason = Some(PreparedPlayoutQueueEventReason::NaturalEnd);
                send_stop_audio_boundary_with_owned_deadline_sender(
                    &mut session,
                    Instant::now() + FRAME_DURATION,
                )
                .await
            } else if interrupted && session.media_started() {
                cleanup_boundary_reason = Some(PreparedPlayoutQueueEventReason::Stop);
                send_stop_audio_boundary_with_owned_deadline_sender(
                    &mut session,
                    Instant::now() + FRAME_DURATION,
                )
                .await
            } else if interrupted {
                session.stop_speaking().await.map_err(RuntimeError::from)
            } else {
                Ok(())
            };
            if restore_to_current_voice {
                *self.voice.lock().await = Some(session);
            }
            self.mark_media_owner_returned();
            stop_result
        } else {
            self.mark_media_owner_returned();
            Ok(())
        };
        if cleanup_result.is_ok()
            && let Some(reason) = cleanup_boundary_reason
        {
            metrics.record_explicit_media_boundary(reason);
        }

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
        if media_owner_was_active {
            self.send_media_command(MediaCommand::Stop {
                account_popped_frame_as_skipped: false,
            })
            .await;
            self.wait_for_media_owner_session(voice_transport_generation)
                .await?;
        }
        self.invalidate_playback();
        self.defer_playback_reset();
        if !media_owner_was_active {
            self.send_media_command(MediaCommand::Stop {
                account_popped_frame_as_skipped: true,
            })
            .await;
            self.wait_for_media_owner_session(voice_transport_generation)
                .await?;
        }
        let mut voice = self.voice.lock().await;
        if let Some(session) = voice.as_mut()
            && session.is_connected()
            && (media_owner_was_active || session.media_started())
        {
            send_stop_audio_boundary_with_owned_deadline_sender(
                session,
                Instant::now() + FRAME_DURATION,
            )
            .await?;
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
        self.send_media_command(MediaCommand::Stop {
            account_popped_frame_as_skipped: true,
        })
        .await;
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
            self.send_media_command(MediaCommand::Stop {
                account_popped_frame_as_skipped: false,
            })
            .await;
            self.wait_for_media_owner_session(voice_transport_generation)
                .await?;
            self.invalidate_playback();
        }
        if self.current_voice_context_matches(&new_voice).await {
            return self
                .rollover_matching_voice_context(new_voice, resume_video_id, rollover_epoch)
                .await;
        }
        self.voice_transport_generation
            .fetch_add(1, Ordering::SeqCst);
        self.invalidate_playback();
        self.send_media_command(MediaCommand::Stop {
            account_popped_frame_as_skipped: false,
        })
        .await;
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
        self.send_media_command(MediaCommand::Stop {
            account_popped_frame_as_skipped: false,
        })
        .await;

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
        self.send_media_command(MediaCommand::Stop {
            account_popped_frame_as_skipped: true,
        })
        .await;
        let mut voice = self.voice.lock().await;
        if let Some(session) = voice.as_mut()
            && session.is_connected()
            && session.media_started()
        {
            let _ = send_stop_audio_boundary_with_owned_deadline_sender(
                session,
                Instant::now() + FRAME_DURATION,
            )
            .await;
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
            egress_buffer: DiscordEgressBuffer::new(),
            prepared_playout_queue: PreparedPlayoutQueue::new(),
            playout_generation: Arc::new(AtomicU64::new(0)),
            prepared_rebuild_credits: VecDeque::new(),
            live_media_delay_for_tests: input.live_media_delay_for_tests,
            account_popped_frame_as_skipped_on_stop: false,
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

    fn current_playout_generation(&self) -> u64 {
        self.playout_generation.load(Ordering::Acquire)
    }

    fn invalidate_prepared_playout_generation(&self) {
        self.playout_generation.fetch_add(1, Ordering::AcqRel);
    }

    fn remember_rebuild_credit(
        &mut self,
        frame: &OpusFrame,
        reason: PreparedPlayoutQueueEventReason,
    ) {
        self.prepared_rebuild_credits
            .push_back(PreparedFrameRebuildCredit {
                identity: prepared_frame_identity(frame),
                reason,
            });
    }

    fn take_rebuild_credit(
        &mut self,
        frame: &OpusFrame,
    ) -> Option<PreparedPlayoutQueueEventReason> {
        let identity = prepared_frame_identity(frame);
        let index = self
            .prepared_rebuild_credits
            .iter()
            .position(|credit| credit.identity == identity)?;
        self.prepared_rebuild_credits
            .remove(index)
            .map(|credit| credit.reason)
    }

    fn record_prepared_playout_event(
        &self,
        event_kind: PreparedPlayoutQueueEventKind,
        reason: PreparedPlayoutQueueEventReason,
        command: &PreparedPlayoutCommand,
        queue_depth_after: OpusBufferDepth,
    ) {
        let event = prepared_playout_queue_event(event_kind, reason, command, queue_depth_after);
        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::PreparedPlayoutQueueEvent(event));
    }

    fn pending_deadline_track_depth(
        pending_deadline_commands: &VecDeque<PendingDeadlineCommand>,
    ) -> OpusBufferDepth {
        pending_deadline_commands
            .iter()
            .fold(OpusBufferDepth::default(), |mut depth, pending| {
                let pending_depth = pending.track_depth();
                depth.packets = depth.packets.saturating_add(pending_depth.packets);
                depth.bytes = depth.bytes.saturating_add(pending_depth.bytes);
                depth.duration_ms = depth.duration_ms.saturating_add(pending_depth.duration_ms);
                depth.duration_samples = depth
                    .duration_samples
                    .saturating_add(pending_depth.duration_samples);
                depth
            })
    }

    fn prepared_playout_depth_with_pending(
        &self,
        pending_deadline_commands: &VecDeque<PendingDeadlineCommand>,
    ) -> OpusBufferDepth {
        let mut depth = self.prepared_playout_queue.depth();
        let pending_depth = Self::pending_deadline_track_depth(pending_deadline_commands);
        depth.packets = depth.packets.saturating_add(pending_depth.packets);
        depth.bytes = depth.bytes.saturating_add(pending_depth.bytes);
        depth.duration_ms = depth.duration_ms.saturating_add(pending_depth.duration_ms);
        depth.duration_samples = depth
            .duration_samples
            .saturating_add(pending_depth.duration_samples);
        depth
    }

    async fn pump_prepared_playout_to_deadline_sender(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
    ) -> Result<(), RuntimeError> {
        while deadline_sender.available_command_capacity() > 0 {
            let Some(prepared) = self.prepared_playout_queue.pop() else {
                break;
            };
            let command = prepared.command;
            self.record_prepared_playout_event(
                PreparedPlayoutQueueEventKind::DequeuedToDeadlineSender,
                PreparedPlayoutQueueEventReason::SteadyPlayback,
                &command,
                self.prepared_playout_queue.depth(),
            );
            let pending = PendingDeadlineCommand::new(&command, Some(prepared.frame));
            deadline_sender.try_send_command(command)?;
            pending_deadline_commands.push_back(pending);
        }
        Ok(())
    }

    async fn replace_deadline_sender_after_invalidation(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        reason: PreparedPlayoutQueueEventReason,
    ) -> Result<Vec<OpusFrame>, RuntimeError> {
        self.replace_deadline_sender_after_invalidation_at(
            deadline_sender,
            pending_deadline_commands,
            reason,
            Instant::now() + FRAME_DURATION,
        )
        .await
    }

    async fn replace_deadline_sender_after_invalidation_at(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        reason: PreparedPlayoutQueueEventReason,
        next_deadline: Instant,
    ) -> Result<Vec<OpusFrame>, RuntimeError> {
        *deadline_sender = LiveDeadlineSender::spawn(
            self.session.cloned_prepared_packet_sender()?,
            Arc::clone(&self.playout_generation),
            next_deadline,
        );
        let mut pending_frames = Vec::new();
        while let Some(mut pending) = pending_deadline_commands.pop_front() {
            let event = pending_prepared_playout_queue_event(
                PreparedPlayoutQueueEventKind::DroppedBeforeSend,
                reason,
                &pending,
                self.prepared_playout_queue.depth(),
            );
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::PreparedPlayoutQueueEvent(event));
            if let Some(frame) = pending.frame.take() {
                pending_frames.push(frame);
            }
        }
        Ok(pending_frames)
    }

    async fn account_deadline_outcome(
        &mut self,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        outcome: LiveDeadlineOutcome,
        remaining_source_depth: OpusBufferDepth,
    ) -> Result<Option<DeadlineSendRecord>, RuntimeError> {
        let Some(mut pending) = pending_deadline_commands.pop_front() else {
            return Err(RuntimeError::InvalidState(
                "deadline sender emitted a record without a pending command",
            ));
        };

        match outcome {
            LiveDeadlineOutcome::Sent(record) => {
                if !pending.matches_sent_record(&record) {
                    return Err(RuntimeError::InvalidState(
                        "deadline sender committed a different prepared command than queued",
                    ));
                }
                Ok(Some(record))
            }
            LiveDeadlineOutcome::Dropped(record) => {
                if !pending.matches_drop_record(&record) {
                    return Err(RuntimeError::InvalidState(
                        "deadline sender dropped a different prepared command than queued",
                    ));
                }
                let event = dropped_prepared_playout_queue_event(
                    PreparedPlayoutQueueEventReason::Interruption,
                    &record,
                    self.prepared_playout_queue.depth(),
                );
                let _ = self
                    .metrics_tx
                    .try_send(PlaybackPipelineMetric::PreparedPlayoutQueueEvent(event));
                if let Some(frame) = pending.frame.take() {
                    self.account_unsent_egress_frame(
                        frame,
                        remaining_source_depth,
                        PreparedPlayoutQueueEventReason::Interruption,
                    )
                    .await;
                }
                Ok(None)
            }
        }
    }

    async fn drain_ready_deadline_outcomes(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        remaining_source_depth: OpusBufferDepth,
    ) -> Result<Vec<DeadlineSendRecord>, RuntimeError> {
        let mut sent_records = Vec::new();
        while let Some(outcome) = deadline_sender.try_next_outcome()? {
            if let Some(record) = self
                .account_deadline_outcome(
                    pending_deadline_commands,
                    outcome,
                    remaining_source_depth,
                )
                .await?
            {
                sent_records.push(record);
            }
        }
        Ok(sent_records)
    }

    fn record_deadline_send_metric(
        &self,
        pacer: &mut AudioPacer,
        packet_index: &mut u64,
        sent_record: DeadlineSendRecord,
        non_send_work_duration: Duration,
        gateway_drain: VoiceGatewayDrainReport,
        remaining_depth: OpusBufferDepth,
        egress_depth: OpusBufferDepth,
    ) {
        let send_started_at = sent_record.send_started_at;
        let sent_at = sent_record.sent_at;
        let send_duration = sent_at.saturating_duration_since(send_started_at);
        let committed_is_track = sent_record.media_frame.is_some();
        let committed_frame_duration = frame_duration_from_samples(sent_record.duration_samples);
        let pacer_mark = pacer.mark_sent(
            if committed_is_track {
                PacedPacketKind::Track
            } else {
                PacedPacketKind::NonTrack
            },
            sent_record.expected_deadline,
            committed_frame_duration,
            send_started_at,
        );

        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::SenderSent(SenderSentMetric {
                packet_index: *packet_index,
                kind: sent_record.kind,
                duration_ms: sent_record.duration_ms,
                duration_samples: sent_record.duration_samples,
                is_track: committed_is_track,
                expected_deadline: sent_record.expected_deadline,
                send_started_at,
                sent_at,
                send_duration,
                rtp_sequence: sent_record.rtp_sequence,
                rtp_timestamp: sent_record.rtp_timestamp,
                protection_nonce: sent_record.protection_nonce,
                media_frame: sent_record.media_frame,
                non_send_work_duration,
                gateway_drain,
                media_clock_reset: pacer_mark.media_clock_reset,
                tempo_rebased: pacer_mark.tempo_rebased,
                remaining_depth,
                egress_depth,
            }));
        *packet_index = (*packet_index).saturating_add(1);
    }

    async fn enqueue_scheduled_silence(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        reason: PreparedPlayoutQueueEventReason,
    ) -> Result<(), RuntimeError> {
        let packet = self
            .session
            .prepare_current_slot_audio_packet(Bytes::from_static(&SILENCE_FRAME), 960)?;
        let command = PreparedPlayoutCommand {
            packet,
            kind: PreparedPacketKind::ScheduledSilence,
            media_frame: None,
            generation: self.current_playout_generation(),
        };
        let pending = PendingDeadlineCommand::new(&command, None);
        self.record_prepared_playout_event(
            PreparedPlayoutQueueEventKind::Enqueued,
            reason,
            &command,
            OpusBufferDepth {
                packets: 1,
                bytes: command.packet.bytes.len(),
                duration_ms: command.packet.duration_ms,
                duration_samples: u64::from(command.packet.duration_samples),
            },
        );
        deadline_sender.try_send_command(command)?;
        pending_deadline_commands.push_back(pending);

        let scheduled_silence_depth = OpusBufferDepth {
            packets: 1,
            bytes: SILENCE_FRAME.len(),
            duration_ms: 20,
            duration_samples: 960,
        };
        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::NonTrackPlayoutQueueDepth {
                command_kind: PlaybackSendCommandKind::ScheduledSilence,
                depth: scheduled_silence_depth,
            });

        Ok(())
    }

    async fn enqueue_dave_recovery_scheduled_silence(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
    ) -> Result<(), RuntimeError> {
        self.enqueue_scheduled_silence(
            deadline_sender,
            pending_deadline_commands,
            PreparedPlayoutQueueEventReason::DaveTransitionRecovery,
        )
        .await
    }

    async fn enqueue_source_underrun_scheduled_silence(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
    ) -> Result<(), RuntimeError> {
        self.enqueue_scheduled_silence(
            deadline_sender,
            pending_deadline_commands,
            PreparedPlayoutQueueEventReason::SourceUnderrun,
        )
        .await
    }

    async fn fill_dave_recovery_silence_deadline_queue(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
    ) -> Result<(), RuntimeError> {
        let mut pending_recovery_silence = pending_deadline_commands
            .iter()
            .filter(|pending| {
                pending.kind == PreparedPacketKind::ScheduledSilence && pending.frame.is_none()
            })
            .count();

        if pending_recovery_silence < DAVE_RECOVERY_DEADLINE_AHEAD_PACKETS
            && deadline_sender.available_command_capacity() > 0
        {
            self.session.prepare_speaking_before_media().await?;
        }

        while pending_recovery_silence < DAVE_RECOVERY_DEADLINE_AHEAD_PACKETS
            && deadline_sender.available_command_capacity() > 0
        {
            self.enqueue_dave_recovery_scheduled_silence(
                deadline_sender,
                pending_deadline_commands,
            )
            .await?;
            pending_recovery_silence += 1;
        }

        Ok(())
    }

    async fn fill_source_underrun_silence_deadline_queue(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
    ) -> Result<(), RuntimeError> {
        let mut pending_source_silence = pending_deadline_commands
            .iter()
            .filter(|pending| {
                pending.kind == PreparedPacketKind::ScheduledSilence && pending.frame.is_none()
            })
            .count();

        if pending_source_silence < SOURCE_UNDERRUN_DEADLINE_AHEAD_PACKETS
            && deadline_sender.available_command_capacity() > 0
        {
            self.session.prepare_speaking_before_media().await?;
        }

        while pending_source_silence < SOURCE_UNDERRUN_DEADLINE_AHEAD_PACKETS
            && deadline_sender.available_command_capacity() > 0
        {
            self.enqueue_source_underrun_scheduled_silence(
                deadline_sender,
                pending_deadline_commands,
            )
            .await?;
            pending_source_silence += 1;
        }

        Ok(())
    }

    async fn send_dave_recovery_scheduled_silence(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        pacer: &mut AudioPacer,
        packet_index: &mut u64,
        gateway_drain: VoiceGatewayDrainReport,
        remaining_source_depth: OpusBufferDepth,
    ) -> Result<(), RuntimeError> {
        self.fill_dave_recovery_silence_deadline_queue(deadline_sender, pending_deadline_commands)
            .await?;

        let outcome = deadline_sender.next_outcome().await?;
        let Some(sent_record) = self
            .account_deadline_outcome(pending_deadline_commands, outcome, remaining_source_depth)
            .await?
        else {
            return Err(RuntimeError::InvalidState(
                "deadline sender dropped DAVE recovery silence",
            ));
        };
        self.record_deadline_send_metric(
            pacer,
            packet_index,
            sent_record,
            Duration::ZERO,
            gateway_drain,
            remaining_source_depth,
            self.prepared_playout_depth_with_pending(pending_deadline_commands),
        );
        Ok(())
    }

    async fn run_loop(&mut self) -> Result<bool, RuntimeError> {
        let mut pacer = AudioPacer::starting_after(FRAME_DURATION);
        let mut deadline_sender = LiveDeadlineSender::spawn(
            self.session.cloned_prepared_packet_sender()?,
            Arc::clone(&self.playout_generation),
            pacer.next_deadline(),
        );
        let mut pending_deadline_commands = VecDeque::new();
        let mut packet_index = 0u64;
        let mut paused = false;
        let mut source_ended = false;
        let mut source_underrun_active = false;
        let mut dave_recovery_active = false;
        let source_depth = current_source_depth(&self.source_buffer).await?;
        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::SenderStarted { source_depth });
        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::EgressDepth(
                self.prepared_playout_queue.depth(),
            ));

        'egress: loop {
            if !self.playback_is_current() {
                self.invalidate_prepared_playout_generation();
                self.account_popped_frame_as_skipped_on_stop = true;
                let pending_frames = self
                    .replace_deadline_sender_after_invalidation(
                        &mut deadline_sender,
                        &mut pending_deadline_commands,
                        PreparedPlayoutQueueEventReason::Interruption,
                    )
                    .await?;
                self.flush_egress_buffer_for_interruption(
                    PreparedPlayoutQueueEventReason::Interruption,
                    pending_frames,
                )
                .await;
                return Ok(false);
            }
            let ready_source_depth = current_source_depth(&self.source_buffer).await?;
            let ready_sent_records = self
                .drain_ready_deadline_outcomes(
                    &mut deadline_sender,
                    &mut pending_deadline_commands,
                    ready_source_depth,
                )
                .await?;
            for sent_record in ready_sent_records {
                self.record_deadline_send_metric(
                    &mut pacer,
                    &mut packet_index,
                    sent_record,
                    Duration::ZERO,
                    VoiceGatewayDrainReport::default(),
                    ready_source_depth,
                    self.prepared_playout_depth_with_pending(&pending_deadline_commands),
                );
            }
            let mut no_selected_prepared = None;
            if let Some(ended) = self
                .drain_commands(
                    &mut deadline_sender,
                    &mut pending_deadline_commands,
                    &mut paused,
                    &mut pacer,
                    &mut no_selected_prepared,
                    &mut packet_index,
                    source_depth,
                )
                .await?
            {
                return Ok(ended);
            }
            if paused {
                if let Some(ended) = self
                    .wait_while_paused(
                        &mut deadline_sender,
                        &mut pending_deadline_commands,
                        &mut paused,
                        &mut pacer,
                        &mut packet_index,
                    )
                    .await?
                {
                    return Ok(ended);
                }
                continue;
            }
            if source_ended && current_source_depth(&self.source_buffer).await?.packets > 0 {
                source_ended = false;
            }

            if dave_recovery_active {
                self.fill_dave_recovery_silence_deadline_queue(
                    &mut deadline_sender,
                    &mut pending_deadline_commands,
                )
                .await?;
            }

            let expected_deadline = if dave_recovery_active {
                Instant::now() + DAVE_RECOVERY_GATEWAY_POLL
            } else {
                pacer.next_deadline()
            };
            let gateway_drain = match self
                .session
                .prepare_media_state_before_slot(expected_deadline)
                .await
            {
                Ok(report) => {
                    dave_recovery_active = false;
                    report
                }
                Err(err) if err.to_string().contains("dave") => {
                    if !dave_recovery_active {
                        dave_recovery_active = true;
                        let _ = self
                            .metrics_tx
                            .try_send(PlaybackPipelineMetric::DaveTransitionReachedBuilder);
                        let _ = self.metrics_tx.try_send(
                            PlaybackPipelineMetric::SenderMediaClockReset {
                                reason: MediaClockResetReason::DaveTransitionRecovery,
                            },
                        );
                        let _ = self.metrics_tx.try_send(
                            PlaybackPipelineMetric::ExplicitMediaBoundary {
                                reason: PreparedPlayoutQueueEventReason::DaveTransitionRecovery,
                            },
                        );
                        let _ = self
                            .metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderStaleDaveSendPrevented);
                        self.invalidate_prepared_playout_generation();
                        self.session.discard_unsent_prepared_packets();
                        let recovery_deadline = expected_deadline
                            .max(Instant::now() + DAVE_RECOVERY_DEADLINE_START_GUARD);
                        let pending_frames = self
                            .replace_deadline_sender_after_invalidation_at(
                                &mut deadline_sender,
                                &mut pending_deadline_commands,
                                PreparedPlayoutQueueEventReason::DaveTransitionRecovery,
                                recovery_deadline,
                            )
                            .await?;
                        self.flush_egress_buffer_for_interruption(
                            PreparedPlayoutQueueEventReason::DaveTransitionRecovery,
                            pending_frames,
                        )
                        .await;
                    }
                    let recovery_source_depth = current_source_depth(&self.source_buffer).await?;
                    self.send_dave_recovery_scheduled_silence(
                        &mut deadline_sender,
                        &mut pending_deadline_commands,
                        &mut pacer,
                        &mut packet_index,
                        VoiceGatewayDrainReport::default(),
                        recovery_source_depth,
                    )
                    .await?;
                    continue;
                }
                Err(err) => return Err(RuntimeError::from(err)),
            };

            let latest_source_depth = self
                .fill_prepared_playout_queue(
                    &mut source_ended,
                    &mut source_underrun_active,
                    Self::pending_deadline_track_depth(&pending_deadline_commands).duration_ms,
                )
                .await?;
            self.pump_prepared_playout_to_deadline_sender(
                &mut deadline_sender,
                &mut pending_deadline_commands,
            )
            .await?;
            if source_underrun_active && !source_ended {
                self.fill_source_underrun_silence_deadline_queue(
                    &mut deadline_sender,
                    &mut pending_deadline_commands,
                )
                .await?;
            }
            let egress_depth_after_pump =
                self.prepared_playout_depth_with_pending(&pending_deadline_commands);
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::EgressDepth(egress_depth_after_pump));
            if !source_ended {
                let _ = self
                    .metrics_tx
                    .try_send(PlaybackPipelineMetric::PreparedTrackQueueDepth(
                        egress_depth_after_pump,
                    ));
            }

            if !self.playback_is_current() {
                self.invalidate_prepared_playout_generation();
                self.session.discard_unsent_prepared_packets();
                self.account_popped_frame_as_skipped_on_stop = true;
                let pending_frames = self
                    .replace_deadline_sender_after_invalidation(
                        &mut deadline_sender,
                        &mut pending_deadline_commands,
                        PreparedPlayoutQueueEventReason::Interruption,
                    )
                    .await?;
                self.flush_egress_buffer_for_interruption(
                    PreparedPlayoutQueueEventReason::Interruption,
                    pending_frames,
                )
                .await;
                return Ok(false);
            }
            let mut no_selected_prepared = None;
            if let Some(ended) = self
                .drain_commands(
                    &mut deadline_sender,
                    &mut pending_deadline_commands,
                    &mut paused,
                    &mut pacer,
                    &mut no_selected_prepared,
                    &mut packet_index,
                    latest_source_depth,
                )
                .await?
            {
                self.invalidate_prepared_playout_generation();
                self.session.discard_unsent_prepared_packets();
                return Ok(ended);
            }
            if paused {
                self.invalidate_prepared_playout_generation();
                self.session.discard_unsent_prepared_packets();
                if let Some(ended) = self
                    .wait_while_paused(
                        &mut deadline_sender,
                        &mut pending_deadline_commands,
                        &mut paused,
                        &mut pacer,
                        &mut packet_index,
                    )
                    .await?
                {
                    return Ok(ended);
                }
                continue 'egress;
            }
            if pending_deadline_commands.is_empty() {
                if source_ended {
                    return Ok(true);
                }
                let _ = self
                    .metrics_tx
                    .try_send(PlaybackPipelineMetric::SenderEgressUnderrun {
                        depth: egress_depth_after_pump,
                    });
                if !source_underrun_active {
                    source_underrun_active = true;
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderSourceUnderrun {
                                depth: latest_source_depth,
                            });
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderMediaClockReset {
                                reason: MediaClockResetReason::SourceUnderrun,
                            });
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::ExplicitMediaBoundary {
                                reason: PreparedPlayoutQueueEventReason::SourceUnderrun,
                            });
                    let _ = self
                        .metrics_tx
                        .try_send(PlaybackPipelineMetric::SourceUnderrunReachedBuilder);
                }
                self.fill_source_underrun_silence_deadline_queue(
                    &mut deadline_sender,
                    &mut pending_deadline_commands,
                )
                .await?;
                let _ = self
                    .metrics_tx
                    .try_send(PlaybackPipelineMetric::SourceUnderrunReachedDeadlineSender);
            }
            if let Some(delay) = self
                .live_media_delay_for_tests
                .as_ref()
                .and_then(|delay_for_packet| delay_for_packet(packet_index))
            {
                tokio::time::sleep(delay).await;
            }
            let outcome = tokio::select! {
                outcome = deadline_sender.next_outcome() => outcome?,
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        return Ok(false);
                    };
                    let mut no_selected_prepared = None;
                    if self.apply_command(
                        command,
                        &mut deadline_sender,
                        &mut pending_deadline_commands,
                        &mut paused,
                        &mut pacer,
                        &mut no_selected_prepared,
                        &mut packet_index,
                        latest_source_depth,
                    ).await? {
                        return Ok(false);
                    }
                    continue 'egress;
                }
            };
            let Some(sent_record) = self
                .account_deadline_outcome(
                    &mut pending_deadline_commands,
                    outcome,
                    latest_source_depth,
                )
                .await?
            else {
                continue;
            };
            self.record_deadline_send_metric(
                &mut pacer,
                &mut packet_index,
                sent_record,
                Duration::ZERO,
                gateway_drain,
                latest_source_depth,
                self.prepared_playout_depth_with_pending(&pending_deadline_commands),
            );
        }
    }

    fn playback_is_current(&self) -> bool {
        self.current_playback_epoch.load(Ordering::SeqCst) == self.playback_epoch
    }

    async fn fill_prepared_playout_queue(
        &mut self,
        source_ended: &mut bool,
        source_underrun_active: &mut bool,
        pending_playout_duration_ms: u64,
    ) -> Result<OpusBufferDepth, RuntimeError> {
        let mut latest_source_depth = self
            .refill_egress_buffer(
                source_ended,
                source_underrun_active,
                pending_playout_duration_ms,
            )
            .await?;

        while (!*source_ended || self.egress_buffer.depth().packets > 0)
            && pending_playout_duration_ms
                .saturating_add(self.prepared_playout_queue.buffered_duration_ms())
                < DISCORD_EGRESS_BUFFER_TARGET_MS
            && !self.prepared_playout_queue.is_full()
        {
            if self.egress_buffer.depth().packets == 0 {
                latest_source_depth = self
                    .refill_egress_buffer(
                        source_ended,
                        source_underrun_active,
                        pending_playout_duration_ms,
                    )
                    .await?;
            }

            let Some(frame) = self.egress_buffer.pop() else {
                break;
            };

            let prepare_started_at = Instant::now();
            self.session.prepare_speaking_before_media().await?;
            let packet = self
                .session
                .prepare_current_slot_audio_packet(frame.data.clone(), frame.duration_samples)?;
            let prepare_duration = prepare_started_at.elapsed();
            let prepared = PreparedTrackPlayout {
                command: PreparedPlayoutCommand {
                    packet,
                    kind: PreparedPacketKind::Track,
                    media_frame: Some(prepared_media_frame(&frame)),
                    generation: self.current_playout_generation(),
                },
                frame,
            };
            let rebuild_reason = self.take_rebuild_credit(&prepared.frame);
            let yield_after_controlled_rebuild = matches!(
                rebuild_reason,
                Some(
                    PreparedPlayoutQueueEventReason::DaveTransitionRecovery
                        | PreparedPlayoutQueueEventReason::SourceUnderrun
                )
            );
            let event_kind = rebuild_reason
                .map(|_| PreparedPlayoutQueueEventKind::Rebuilt)
                .unwrap_or(PreparedPlayoutQueueEventKind::Enqueued);
            let event_reason =
                rebuild_reason.unwrap_or(PreparedPlayoutQueueEventReason::SteadyPlayback);
            self.prepared_playout_queue.push(prepared).map_err(|_| {
                RuntimeError::InvalidState("prepared playout queue exceeded high watermark")
            })?;
            if let Some(prepared) = self.prepared_playout_queue.queue.back() {
                self.record_prepared_playout_event(
                    event_kind,
                    event_reason,
                    &prepared.command,
                    self.prepared_playout_queue.depth(),
                );
            }
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::PlayoutBuilderPrepared {
                    duration: prepare_duration,
                });
            if yield_after_controlled_rebuild {
                tokio::task::yield_now().await;
            }
            latest_source_depth = self
                .refill_egress_buffer(
                    source_ended,
                    source_underrun_active,
                    pending_playout_duration_ms,
                )
                .await?;
        }

        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::EgressDepth(
                self.prepared_playout_queue.depth(),
            ));
        Ok(latest_source_depth)
    }

    async fn drain_commands(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        paused: &mut bool,
        pacer: &mut AudioPacer,
        selected_prepared: &mut Option<PreparedTrackPlayout>,
        packet_index: &mut u64,
        remaining_source_depth: OpusBufferDepth,
    ) -> Result<Option<bool>, RuntimeError> {
        while let Ok(command) = self.command_rx.try_recv() {
            if self
                .apply_command(
                    command,
                    deadline_sender,
                    pending_deadline_commands,
                    paused,
                    pacer,
                    selected_prepared,
                    packet_index,
                    remaining_source_depth,
                )
                .await?
            {
                return Ok(Some(false));
            }
        }
        Ok(None)
    }

    async fn account_unsent_egress_frame(
        &mut self,
        frame: OpusFrame,
        remaining_source_depth: OpusBufferDepth,
        reason: PreparedPlayoutQueueEventReason,
    ) {
        let duration_ms = frame.duration_ms;
        let mut restored = true;
        let mut frame_for_credit = Some(frame.clone());
        if let Err(frame) = self.egress_buffer.push_front(frame) {
            if let Err(frame) =
                restore_live_source_frame(&self.source_buffer, frame, &self.metrics_tx).await
            {
                restored = false;
                frame_for_credit = None;
                if !self.account_popped_frame_as_skipped_on_stop {
                    let _ = self.metrics_tx.try_send(
                        PlaybackPipelineMetric::SenderSkippedSourceFrames {
                            frame_count: 1,
                            duration_ms: frame.duration_ms,
                            remaining_depth: remaining_source_depth,
                        },
                    );
                }
                let _ =
                    self.metrics_tx
                        .try_send(PlaybackPipelineMetric::EgressDroppedMusicFrames {
                            frame_count: 1,
                            duration_ms: frame.duration_ms,
                        });
                let _ = self
                    .metrics_tx
                    .try_send(PlaybackPipelineMetric::DiscardedSourceFrames {
                        reason,
                        frame_count: 1,
                        duration_ms: frame.duration_ms,
                    });
            }
        }
        if restored {
            if let Some(frame) = frame_for_credit.as_ref() {
                self.remember_rebuild_credit(frame, reason);
            }
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::RestoredSourceFrames {
                    frame_count: 1,
                    duration_ms,
                });
        }
        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::EgressDepth(
                self.prepared_playout_queue.depth(),
            ));
    }

    async fn flush_egress_buffer_for_interruption(
        &mut self,
        reason: PreparedPlayoutQueueEventReason,
        mut frames: Vec<OpusFrame>,
    ) {
        frames.extend(self.drain_egress_frames(reason));
        if frames.is_empty() {
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::EgressDepth(
                    self.prepared_playout_queue.depth(),
                ));
            return;
        }

        let mut skipped_count = 0u64;
        let mut skipped_duration_ms = 0u64;
        let (mut restored_count, mut restored_duration_ms, source_restore_frames) =
            restore_interrupted_frames_to_egress_buffer(
                &mut self.egress_buffer,
                &mut self.prepared_rebuild_credits,
                reason,
                frames,
            );
        let mut discarded_count = 0u64;
        let mut discarded_duration_ms = 0u64;
        let mut remaining_depth = current_source_depth(&self.source_buffer)
            .await
            .unwrap_or_default();
        for frame in source_restore_frames.into_iter().rev() {
            let restored_frame = frame.clone();
            match restore_live_source_frame(&self.source_buffer, frame, &self.metrics_tx).await {
                Ok(depth) => {
                    remaining_depth = depth;
                    restored_count = restored_count.saturating_add(1);
                    restored_duration_ms =
                        restored_duration_ms.saturating_add(restored_frame.duration_ms);
                    self.remember_rebuild_credit(&restored_frame, reason);
                }
                Err(frame) => {
                    discarded_count = discarded_count.saturating_add(1);
                    discarded_duration_ms = discarded_duration_ms.saturating_add(frame.duration_ms);
                    if !self.account_popped_frame_as_skipped_on_stop {
                        skipped_count = skipped_count.saturating_add(1);
                        skipped_duration_ms = skipped_duration_ms.saturating_add(frame.duration_ms);
                    }
                }
            }
        }
        if skipped_count > 0 {
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::SenderSkippedSourceFrames {
                    frame_count: skipped_count,
                    duration_ms: skipped_duration_ms,
                    remaining_depth,
                });
        }
        if restored_count > 0 {
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::RestoredSourceFrames {
                    frame_count: restored_count,
                    duration_ms: restored_duration_ms,
                });
        }
        if discarded_count > 0 {
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::DiscardedSourceFrames {
                    reason,
                    frame_count: discarded_count,
                    duration_ms: discarded_duration_ms,
                });
            let _ = self
                .metrics_tx
                .try_send(PlaybackPipelineMetric::EgressDroppedMusicFrames {
                    frame_count: discarded_count,
                    duration_ms: discarded_duration_ms,
                });
        }
        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::EgressDepth(
                self.prepared_playout_queue.depth(),
            ));
    }

    fn drain_egress_frames(&mut self, reason: PreparedPlayoutQueueEventReason) -> Vec<OpusFrame> {
        let mut frames = Vec::new();
        while let Some(prepared) = self.prepared_playout_queue.pop() {
            self.record_prepared_playout_event(
                PreparedPlayoutQueueEventKind::DroppedBeforeSend,
                reason,
                &prepared.command,
                self.prepared_playout_queue.depth(),
            );
            frames.push(prepared.frame);
        }
        while let Some(frame) = self.egress_buffer.pop() {
            frames.push(frame);
        }
        frames
    }

    async fn wait_while_paused(
        &mut self,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        paused: &mut bool,
        pacer: &mut AudioPacer,
        packet_index: &mut u64,
    ) -> Result<Option<bool>, RuntimeError> {
        while *paused {
            if !self.playback_is_current() {
                return Ok(Some(false));
            }
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        return Ok(Some(false));
                    };
                    let mut no_selected_prepared = None;
                    if self.apply_command(
                        command,
                        deadline_sender,
                        pending_deadline_commands,
                        paused,
                        pacer,
                        &mut no_selected_prepared,
                        packet_index,
                        OpusBufferDepth::default(),
                    ).await? {
                        return Ok(Some(false));
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        Ok(None)
    }

    async fn refill_egress_buffer(
        &mut self,
        source_ended: &mut bool,
        source_underrun_active: &mut bool,
        pending_playout_duration_ms: u64,
    ) -> Result<OpusBufferDepth, RuntimeError> {
        let mut latest_source_depth = current_source_depth(&self.source_buffer).await?;
        let raw_refill_target_ms = DISCORD_EGRESS_BUFFER_TARGET_MS
            .saturating_sub(pending_playout_duration_ms)
            .saturating_sub(self.prepared_playout_queue.buffered_duration_ms());

        while !*source_ended
            && self.egress_buffer.buffered_duration_ms() < raw_refill_target_ms
            && !self.egress_buffer.is_full()
        {
            let source_poll =
                pop_live_source_frame(&self.source_buffer, self.playback_epoch, &self.metrics_tx)
                    .await?;
            latest_source_depth = source_poll.depth;
            let Some((frame, remaining_depth)) = source_poll.frame else {
                if source_poll.ended {
                    *source_ended = true;
                }
                break;
            };

            if *source_underrun_active {
                *source_underrun_active = false;
                let _ = self.metrics_tx.try_send(
                    PlaybackPipelineMetric::SenderResumedAfterSourceUnderrun {
                        depth: remaining_depth,
                    },
                );
            }

            if let Err(frame) = self.egress_buffer.push(frame) {
                match restore_live_source_frame(&self.source_buffer, frame, &self.metrics_tx).await
                {
                    Ok(depth) => latest_source_depth = depth,
                    Err(frame) => {
                        let _ = self.metrics_tx.try_send(
                            PlaybackPipelineMetric::SenderSkippedSourceFrames {
                                frame_count: 1,
                                duration_ms: frame.duration_ms,
                                remaining_depth,
                            },
                        );
                        let _ = self.metrics_tx.try_send(
                            PlaybackPipelineMetric::EgressDroppedMusicFrames {
                                frame_count: 1,
                                duration_ms: frame.duration_ms,
                            },
                        );
                    }
                }
                break;
            }
            latest_source_depth = remaining_depth;
        }

        let _ = self
            .metrics_tx
            .try_send(PlaybackPipelineMetric::EgressDepth(
                self.prepared_playout_queue.depth(),
            ));
        Ok(latest_source_depth)
    }

    async fn apply_command(
        &mut self,
        command: MediaCommand,
        deadline_sender: &mut LiveDeadlineSender,
        pending_deadline_commands: &mut VecDeque<PendingDeadlineCommand>,
        paused: &mut bool,
        pacer: &mut AudioPacer,
        selected_prepared: &mut Option<PreparedTrackPlayout>,
        packet_index: &mut u64,
        remaining_source_depth: OpusBufferDepth,
    ) -> Result<bool, RuntimeError> {
        match command {
            MediaCommand::Pause => {
                if !*paused {
                    self.invalidate_prepared_playout_generation();
                    self.session.discard_unsent_prepared_packets();
                    if let Some(prepared) = selected_prepared.take() {
                        self.account_unsent_egress_frame(
                            prepared.frame,
                            remaining_source_depth,
                            PreparedPlayoutQueueEventReason::Pause,
                        )
                        .await;
                    }
                    let pending_frames = self
                        .replace_deadline_sender_after_invalidation(
                            deadline_sender,
                            pending_deadline_commands,
                            PreparedPlayoutQueueEventReason::Pause,
                        )
                        .await?;
                    self.flush_egress_buffer_for_interruption(
                        PreparedPlayoutQueueEventReason::Pause,
                        pending_frames,
                    )
                    .await;
                    let boundary_generation = self.current_playout_generation();
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::ExplicitMediaBoundary {
                                reason: PreparedPlayoutQueueEventReason::Pause,
                            });
                    let _ = self.metrics_tx.try_send(
                        PlaybackPipelineMetric::NonTrackPlayoutQueueDepth {
                            command_kind: PlaybackSendCommandKind::BoundarySilence,
                            depth: OpusBufferDepth {
                                packets: 1,
                                bytes: SILENCE_FRAME.len(),
                                duration_ms: 20,
                                duration_samples: 960,
                            },
                        },
                    );
                    let boundary_records = send_stop_audio_boundary_through_deadline_sender(
                        &mut self.session,
                        deadline_sender,
                        boundary_generation,
                    )
                    .await?;
                    for sent_record in boundary_records {
                        self.record_deadline_send_metric(
                            pacer,
                            packet_index,
                            sent_record,
                            Duration::ZERO,
                            VoiceGatewayDrainReport::default(),
                            remaining_source_depth,
                            self.prepared_playout_depth_with_pending(pending_deadline_commands),
                        );
                    }
                    *paused = true;
                    pacer.reset_deadline();
                    let _ =
                        self.metrics_tx
                            .try_send(PlaybackPipelineMetric::SenderMediaClockReset {
                                reason: MediaClockResetReason::PauseResume,
                            });
                }
                Ok(false)
            }
            MediaCommand::Resume => {
                if *paused {
                    *paused = false;
                    self.session.prepare_speaking_before_media().await?;
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
                Ok(false)
            }
            MediaCommand::Stop {
                account_popped_frame_as_skipped,
            } => {
                self.account_popped_frame_as_skipped_on_stop = account_popped_frame_as_skipped;
                self.invalidate_prepared_playout_generation();
                self.session.discard_unsent_prepared_packets();
                let pending_frames = self
                    .replace_deadline_sender_after_invalidation(
                        deadline_sender,
                        pending_deadline_commands,
                        PreparedPlayoutQueueEventReason::Stop,
                    )
                    .await?;
                self.flush_egress_buffer_for_interruption(
                    PreparedPlayoutQueueEventReason::Stop,
                    pending_frames,
                )
                .await;
                Ok(true)
            }
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

async fn restore_live_source_frame(
    source_buffer: &Arc<SharedSourceBuffer>,
    frame: OpusFrame,
    metrics_tx: &mpsc::Sender<PlaybackPipelineMetric>,
) -> Result<OpusBufferDepth, OpusFrame> {
    let depth = {
        let mut source_state = source_buffer.state.lock().await;
        source_state.queue.push_front(frame)?;
        source_state.queue.depth()
    };
    source_buffer.changed.notify_waiters();
    let _ = metrics_tx.try_send(PlaybackPipelineMetric::SourceDepth(depth));
    Ok(depth)
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

fn playback_send_command_kind(kind: PreparedPacketKind) -> PlaybackSendCommandKind {
    match kind {
        PreparedPacketKind::Track => PlaybackSendCommandKind::Track,
        PreparedPacketKind::ScheduledSilence => PlaybackSendCommandKind::ScheduledSilence,
        PreparedPacketKind::BoundarySilence => PlaybackSendCommandKind::BoundarySilence,
    }
}

fn prepared_media_frame(frame: &OpusFrame) -> PreparedMediaFrame {
    PreparedMediaFrame {
        duration_ms: frame.duration_ms,
        duration_samples: frame.duration_samples,
        media_position_ms: frame.source_position_ms,
        media_byte_position: frame.source_byte_position,
        epoch: frame.epoch,
    }
}

fn prepared_frame_identity(frame: &OpusFrame) -> PreparedFrameIdentity {
    PreparedFrameIdentity {
        epoch: frame.epoch,
        source_position_ms: frame.source_position_ms,
        source_byte_position: frame.source_byte_position,
    }
}

fn prepared_playout_queue_event(
    event_kind: PreparedPlayoutQueueEventKind,
    reason: PreparedPlayoutQueueEventReason,
    command: &PreparedPlayoutCommand,
    queue_depth_after: OpusBufferDepth,
) -> PreparedPlayoutQueueEventSnapshot {
    PreparedPlayoutQueueEventSnapshot {
        event_index: 0,
        event_kind,
        reason,
        command_kind: playback_send_command_kind(command.kind),
        media_duration_ms: command.packet.duration_ms,
        media_duration_samples: command.packet.duration_samples,
        rtp_sequence: u32::from(command.packet.rtp_sequence),
        rtp_timestamp: command.packet.rtp_timestamp,
        protection_nonce: command.packet.protection_nonce,
        source_frame_epoch: command.media_frame.map(|frame| frame.epoch),
        source_media_position_ms: command.media_frame.map(|frame| frame.media_position_ms),
        source_media_byte_position: command
            .media_frame
            .and_then(|frame| frame.media_byte_position),
        queue_depth_after: queue_depth_after.into(),
    }
}

fn dropped_prepared_playout_queue_event(
    reason: PreparedPlayoutQueueEventReason,
    drop: &DeadlineDropRecord,
    queue_depth_after: OpusBufferDepth,
) -> PreparedPlayoutQueueEventSnapshot {
    PreparedPlayoutQueueEventSnapshot {
        event_index: 0,
        event_kind: PreparedPlayoutQueueEventKind::DroppedBeforeSend,
        reason,
        command_kind: playback_send_command_kind(drop.kind),
        media_duration_ms: drop.duration_ms,
        media_duration_samples: drop.duration_samples,
        rtp_sequence: u32::from(drop.rtp_sequence),
        rtp_timestamp: drop.rtp_timestamp,
        protection_nonce: drop.protection_nonce,
        source_frame_epoch: drop.media_frame.map(|frame| frame.epoch),
        source_media_position_ms: drop.media_frame.map(|frame| frame.media_position_ms),
        source_media_byte_position: drop.media_frame.and_then(|frame| frame.media_byte_position),
        queue_depth_after: queue_depth_after.into(),
    }
}

fn pending_prepared_playout_queue_event(
    event_kind: PreparedPlayoutQueueEventKind,
    reason: PreparedPlayoutQueueEventReason,
    pending: &PendingDeadlineCommand,
    queue_depth_after: OpusBufferDepth,
) -> PreparedPlayoutQueueEventSnapshot {
    PreparedPlayoutQueueEventSnapshot {
        event_index: 0,
        event_kind,
        reason,
        command_kind: playback_send_command_kind(pending.kind),
        media_duration_ms: pending.duration_ms,
        media_duration_samples: pending.duration_samples,
        rtp_sequence: u32::from(pending.rtp_sequence),
        rtp_timestamp: pending.rtp_timestamp,
        protection_nonce: pending.protection_nonce,
        source_frame_epoch: pending.media_frame.map(|frame| frame.epoch),
        source_media_position_ms: pending.media_frame.map(|frame| frame.media_position_ms),
        source_media_byte_position: pending
            .media_frame
            .and_then(|frame| frame.media_byte_position),
        queue_depth_after: queue_depth_after.into(),
    }
}

async fn send_stop_audio_boundary_with_owned_deadline_sender(
    session: &mut ConnectedVoiceSession,
    next_deadline: Instant,
) -> Result<(), RuntimeError> {
    let mut deadline_sender = LiveDeadlineSender::spawn(
        session.cloned_prepared_packet_sender()?,
        Arc::new(AtomicU64::new(0)),
        next_deadline,
    );
    let _records =
        send_stop_audio_boundary_through_deadline_sender(session, &mut deadline_sender, 0).await?;
    deadline_sender.shutdown().await
}

async fn send_stop_audio_boundary_through_deadline_sender(
    session: &mut ConnectedVoiceSession,
    deadline_sender: &mut LiveDeadlineSender,
    generation: u64,
) -> Result<Vec<DeadlineSendRecord>, RuntimeError> {
    session.prepare_speaking_before_media().await?;
    let mut sent_records = Vec::with_capacity(STOP_AUDIO_BOUNDARY_SILENCE_PACKETS);
    for _ in 0..STOP_AUDIO_BOUNDARY_SILENCE_PACKETS {
        let packet =
            session.prepare_current_slot_audio_packet(Bytes::from_static(&SILENCE_FRAME), 960)?;
        deadline_sender
            .send_command(PreparedPlayoutCommand {
                packet,
                kind: PreparedPacketKind::BoundarySilence,
                media_frame: None,
                generation,
            })
            .await?;
        match deadline_sender.next_outcome().await? {
            LiveDeadlineOutcome::Sent(record) => {
                if record.kind != PreparedPacketKind::BoundarySilence {
                    return Err(RuntimeError::InvalidState(
                        "deadline sender committed a non-boundary command during stop boundary",
                    ));
                }
                sent_records.push(record);
            }
            LiveDeadlineOutcome::Dropped(_) => {
                return Err(RuntimeError::InvalidState(
                    "deadline sender dropped stop boundary silence",
                ));
            }
        }
    }
    if sent_records.len() != STOP_AUDIO_BOUNDARY_SILENCE_PACKETS {
        return Err(RuntimeError::InvalidState(
            "deadline sender did not send the complete stop boundary",
        ));
    }
    session.stop_speaking().await?;
    Ok(sent_records)
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
    fn playback_routes_prepared_playout_commands_through_deadline_sender() {
        let source = include_str!("runtime.rs");
        assert!(
            source.contains("PreparedPlayoutCommand"),
            "active runtime playback should build immutable prepared playout commands"
        );
        assert!(
            source.contains("DeadlineSender::new_with_next_deadline"),
            "active runtime playback should route prepared commands through the deadline sender"
        );
        assert!(
            !source.contains(&["send_current_slot", "_packet(packet).await"].concat()),
            "active runtime playback must not send RTP through the broad voice session"
        );
        assert!(
            !source.contains(&[".stop_", "audio().await"].concat()),
            "runtime playback boundaries must not send RTP silence through the broad voice session"
        );
    }

    #[test]
    fn playback_uses_send_only_voice_sink_for_deadline_sender() {
        let source = include_str!("runtime.rs");
        assert!(
            source.contains("cloned_prepared_packet_sender()?"),
            "deadline sender should own only a cloned send-only prepared-packet sink"
        );
        assert!(
            !source.contains(&["session.", "prepared_packet_sender()?"].concat()),
            "runtime deadline sender must not borrow the broad session/transport at the send boundary"
        );
        assert!(
            !source.contains(
                "DeadlineSender::new_with_next_deadline(\n                &mut self.session"
            ),
            "deadline sender must not be constructed with the broad connected voice session"
        );
    }

    #[test]
    fn stop_audio_boundaries_are_prepared_commands_for_deadline_sender() {
        let source = include_str!("runtime.rs");
        let helper = source
            .split("async fn send_stop_audio_boundary_through_deadline_sender")
            .nth(1)
            .and_then(|tail| tail.split("fn record_producer_sample_for_playback").next())
            .expect("runtime should have a stop-boundary helper");

        assert!(
            helper.contains("PreparedPacketKind::BoundarySilence"),
            "stop/pause boundary RTP silence must be tagged as boundary silence"
        );
        assert!(
            helper.contains(".send_command(PreparedPlayoutCommand"),
            "stop/pause boundary RTP silence must pass through the live deadline sender"
        );
        assert!(
            helper.contains("STOP_AUDIO_BOUNDARY_SILENCE_PACKETS"),
            "stop/pause boundaries should keep the explicit five-packet Discord silence tail"
        );
        assert!(
            !source.contains(&[".send_audio_", "frame("].concat()),
            "runtime must not send RTP through the broad voice audio-frame helper"
        );
    }

    #[test]
    fn skipped_source_metrics_do_not_advance_heard_position() {
        let source = include_str!("runtime.rs");
        let skipped_arm = source
            .split("metrics.record_skipped_source_frames(frame_count, duration_ms);")
            .nth(1)
            .and_then(|tail| {
                tail.split("PlaybackPipelineMetric::SenderResumedAfterSourceUnderrun")
                    .next()
            })
            .expect("runtime metrics loop should handle skipped source frames");

        assert!(
            !skipped_arm.contains("record_sent_packet"),
            "skipped source frames must not be recorded as heard media"
        );
        assert!(
            !skipped_arm.contains("position_ms = position_ms.saturating_add"),
            "skipped source frames must not advance runtime playback position"
        );
    }

    #[test]
    fn live_driver_advances_heard_media_only_from_deadline_send_commit_identity() {
        let source = include_str!("runtime.rs");
        let commit_accounting = source
            .split("fn record_deadline_send_metric")
            .nth(1)
            .and_then(|tail| tail.split("async fn run_loop").next())
            .expect("runtime should record deadline send commits");

        assert!(
            commit_accounting.contains("sent_record.media_frame.is_some()"),
            "live driver should classify heard track media from deadline sender commit identity"
        );
        assert!(
            commit_accounting.contains("duration_ms: sent_record.duration_ms"),
            "live driver should report heard duration from the deadline sender commit record"
        );
        assert!(
            !commit_accounting.contains("duration_ms: frame_duration_ms"),
            "live driver must not advance heard duration from pre-send selected frame metadata"
        );
    }

    #[test]
    fn live_driver_stamps_and_invalidates_prepared_playout_generations() {
        let source = include_str!("runtime.rs");
        assert!(
            source.contains("playout_generation: Arc<AtomicU64>"),
            "live driver should own an explicit prepared playout generation token"
        );
        assert!(
            source.contains("generation: self.current_playout_generation()"),
            "prepared track/silence commands should be stamped with the active generation"
        );
        assert!(
            source.contains("new_with_next_deadline_records_and_generation"),
            "production deadline sender path should enforce the active generation"
        );
        assert!(
            source
                .matches("invalidate_prepared_playout_generation()")
                .count()
                >= 4,
            "pause/stop/currentness/DAVE recovery should invalidate old prepared commands"
        );
    }

    #[test]
    fn live_driver_uses_one_spawned_deadline_sender_task() {
        let source = include_str!("runtime.rs");
        let state = source
            .split("struct LiveDeadlineSender {")
            .nth(1)
            .and_then(|tail| tail.split("}\n\nimpl LiveDeadlineSender").next())
            .expect("runtime should have a live deadline sender handle");

        for required_field in [
            "command_tx: mpsc::Sender<PreparedPlayoutCommand>",
            "send_record_rx: mpsc::Receiver<DeadlineSendRecord>",
            "drop_record_rx: mpsc::Receiver<DeadlineDropRecord>",
            "shutdown: Arc<AtomicBool>",
            "task: Option<JoinHandle<Result<DeadlineSenderMetrics, RuntimeError>>>",
        ] {
            assert!(
                state.contains(required_field),
                "live deadline sender handle should own field: {required_field}"
            );
        }

        let spawn = source
            .split("fn spawn(")
            .nth(1)
            .and_then(|tail| tail.split("async fn send_command").next())
            .expect("runtime should spawn the live deadline sender");
        assert!(
            spawn.contains("sink: VoicePreparedPacketSender"),
            "live deadline sender task should be constructed from a narrow send-only sink"
        );
        assert!(
            spawn.contains("tokio::spawn(async move { sender.run().await })"),
            "deadline sender should run as a long-lived task"
        );
    }

    #[test]
    fn live_driver_feeds_current_packet_to_spawned_deadline_sender() {
        let source = include_str!("runtime.rs");
        let run_loop = source
            .split("async fn run_loop")
            .nth(1)
            .and_then(|tail| tail.split("async fn fill_prepared_playout_queue").next())
            .expect("runtime should have a live media run loop");
        assert!(
            !run_loop
                .contains(&[".send_audio_frame", "_with_duration_samples(frame.data"].concat()),
            "live driver must not call the combined voice send path after the RTP boundary"
        );

        let prepare_state = run_loop
            .find("prepare_media_state_before_slot(expected_deadline)")
            .expect("live loop should prepare media state before the slot");
        let fill_prepared_queue = run_loop
            .find("fill_prepared_playout_queue(")
            .expect("live loop should fill a prepared playout queue before the boundary");
        let hot_send = run_loop
            .find("pump_prepared_playout_to_deadline_sender(")
            .expect("live loop should feed prepared packets to the spawned sender");
        let send_commit = run_loop
            .find("outcome = deadline_sender.next_outcome()")
            .expect("live loop should account sends only from deadline sender records");

        assert!(prepare_state < hot_send);
        assert!(fill_prepared_queue < hot_send);
        assert!(hot_send < send_commit);
        assert!(
            !run_loop.contains("pacer.wait_until_ready().await"),
            "live driver must not own the 20ms RTP sleep once the deadline sender is spawned"
        );
        assert!(
            !run_loop.contains("DeadlineSender::new_with_next_deadline"),
            "live driver must not construct a per-tick deadline sender inside the egress loop"
        );
        assert!(
            !run_loop.contains(&["send_current_slot", "_packet(packet)"].concat()),
            "live driver must not send the current-slot packet through the broad session path"
        );
    }

    #[test]
    fn egress_refill_resumes_source_underrun_before_buffering_frame() {
        let source = include_str!("runtime.rs");
        let refill = source
            .find("async fn refill_egress_buffer")
            .expect("live driver should refill a Discord egress buffer");
        let refill_body = &source[refill..];
        let resumed = refill_body
            .find("if *source_underrun_active")
            .expect("refill should report source underrun recovery");
        let push = refill_body
            .find("self.egress_buffer.push(frame)")
            .expect("refill should move raw source frames into egress");

        assert!(
            resumed < push,
            "source underrun recovery should be reported before the resumed frame enters egress"
        );
        assert!(
            !source.contains(&["async fn ", "wait_for_source_or_control"].concat()),
            "egress underrun should be handled by the clocked egress loop, not a source-blocking sender loop"
        );
    }

    #[test]
    fn source_underrun_silence_stays_ahead_of_deadline_sender() {
        let source = include_str!("runtime.rs");
        assert!(
            source.contains("const SOURCE_UNDERRUN_DEADLINE_AHEAD_PACKETS: usize = 3;"),
            "source underrun should keep multiple scheduled-silence packets ahead of the deadline sender"
        );

        let fill = source
            .find("async fn fill_source_underrun_silence_deadline_queue")
            .expect("live driver should top up source-underrun scheduled silence");
        let fill_body = source[fill..]
            .split("async fn send_dave_recovery_scheduled_silence")
            .next()
            .expect("source underrun silence fill body should be bounded");
        for required in [
            "pending_source_silence < SOURCE_UNDERRUN_DEADLINE_AHEAD_PACKETS",
            "deadline_sender.available_command_capacity() > 0",
            "enqueue_source_underrun_scheduled_silence",
        ] {
            assert!(
                fill_body.contains(required),
                "source underrun silence fill should contain {required}"
            );
        }

        let run_loop = source
            .split("async fn run_loop")
            .nth(1)
            .and_then(|tail| tail.split("async fn fill_prepared_playout_queue").next())
            .expect("runtime should have a live media run loop");
        let underrun_branch = run_loop
            .split("PlaybackPipelineMetric::SenderEgressUnderrun")
            .nth(1)
            .and_then(|tail| tail.split("live_media_delay_for_tests").next())
            .expect("run loop should have a source-underrun branch");
        assert!(
            underrun_branch.contains("fill_source_underrun_silence_deadline_queue("),
            "source underrun should fill a scheduled-silence reservoir, not enqueue one packet"
        );
        assert!(
            !underrun_branch
                .contains("prepare_current_slot_audio_packet(Bytes::from_static(&SILENCE_FRAME)"),
            "source underrun branch must not regress to one inline silence packet per loop"
        );
    }

    #[test]
    fn discord_egress_buffer_is_bounded_raw_opus_fifo() {
        let mut buffer = DiscordEgressBuffer::new();

        for index in 0..25u8 {
            buffer
                .push(
                    OpusFrame::with_duration_samples(Bytes::from(vec![index]), 20, 960)
                        .with_metadata(u64::from(index) * 20, Some(u64::from(index)), 11),
                )
                .expect("500ms egress buffer should accept twenty-five 20ms frames");
        }

        let depth = buffer.depth();
        assert_eq!(DISCORD_EGRESS_BUFFER_TARGET_MS, 400);
        assert_eq!(DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS, 500);
        assert_eq!(DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS, 300);
        assert_eq!(depth.duration_ms, DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS);
        assert_eq!(depth.duration_samples, 24_000);
        assert!(
            buffer
                .push(OpusFrame::with_duration_samples(
                    Bytes::from_static(b"overflow"),
                    20,
                    960
                ))
                .is_err(),
            "egress overflow must backpressure instead of silently dropping frames"
        );

        let first = buffer.pop().expect("egress should preserve FIFO order");
        assert_eq!(first.data.as_ref(), &[0]);
        assert_eq!(first.duration_samples, 960);
        assert_eq!(first.source_position_ms, 0);
        assert_eq!(first.source_byte_position, Some(0));
        assert_eq!(first.epoch, 11);

        let second = buffer.pop().expect("egress should preserve frame metadata");
        assert_eq!(second.data.as_ref(), &[1]);
        assert_eq!(second.source_position_ms, 20);
        assert_eq!(second.source_byte_position, Some(1));
        assert_eq!(second.epoch, 11);
    }

    #[test]
    fn interruption_restore_keeps_unsent_frames_in_source_order() {
        let mut buffer = DiscordEgressBuffer::new();
        let mut rebuild_credits = VecDeque::new();
        let interrupted_frames = (0..20u64)
            .map(|index| {
                OpusFrame::with_duration_samples(Bytes::from(vec![index as u8]), 20, 960)
                    .with_metadata(index * 20, Some(index), 12)
            })
            .collect::<Vec<_>>();

        let (restored_count, restored_duration_ms, source_restore_frames) =
            restore_interrupted_frames_to_egress_buffer(
                &mut buffer,
                &mut rebuild_credits,
                PreparedPlayoutQueueEventReason::Pause,
                interrupted_frames,
            );

        assert_eq!(restored_count, 20);
        assert_eq!(restored_duration_ms, 400);
        assert!(
            source_restore_frames.is_empty(),
            "a normal 400ms prepared reservoir should restore without overflowing egress"
        );

        let restored_positions = std::iter::from_fn(|| buffer.pop())
            .map(|frame| frame.source_position_ms)
            .collect::<Vec<_>>();
        assert_eq!(
            restored_positions,
            (0..20u64).map(|index| index * 20).collect::<Vec<_>>(),
            "pause must not resume unsent prepared frames in reverse order"
        );
        let credited_positions = rebuild_credits
            .iter()
            .map(|credit| credit.identity.source_position_ms)
            .collect::<Vec<_>>();
        assert_eq!(credited_positions, restored_positions);
        assert!(
            rebuild_credits
                .iter()
                .all(|credit| credit.reason == PreparedPlayoutQueueEventReason::Pause)
        );
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
        assert_eq!(frame.source_position_ms, 0);
        assert_eq!(frame.epoch, 7);
        assert_eq!(frame.data.len(), 8);
        assert_eq!(remaining_depth.duration_ms, 4_980);

        let poll = pop_live_source_frame(&source_buffer, 7, &metrics_tx)
            .await
            .expect("live source pop should preserve following raw frame");
        let (frame, remaining_depth) = poll.frame.expect("source buffer should provide raw frame");
        assert_eq!(frame.duration_ms, 20);
        assert_eq!(frame.duration_samples, 960);
        assert_eq!(frame.source_position_ms, 20);
        assert_eq!(frame.epoch, 7);
        assert_eq!(frame.data.len(), 8);
        assert_eq!(remaining_depth.duration_ms, 4_960);
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
            DISCORD_EGRESS_BUFFER_TARGET_MS,
            DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS,
            DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS,
            PlaybackRecoveryMetrics::default(),
        );

        let metrics = collector.snapshot(0, 0, 0, false);

        assert_eq!(
            metrics.source_buffer_target_ms,
            PLAYBACK_SOURCE_BUFFER_TARGET_MS
        );
        assert_eq!(metrics.current_source_buffer_depth.duration_ms, 5_000);
        assert_eq!(metrics.max_source_buffer_depth.duration_samples, 240_000);
        assert_eq!(metrics.egress_buffer_target_ms, 400);
        assert_eq!(metrics.current_egress_buffer_depth.duration_ms, 0);
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
                .push(
                    OpusFrame::with_duration_samples(vec![index as u8; 8].into(), 20, 960)
                        .with_metadata(index as u64 * 20, None, 0),
                )
                .expect("test source buffer should accept frame");
        }
        Arc::new(SharedSourceBuffer::new(queue, end_of_stream))
    }
}
