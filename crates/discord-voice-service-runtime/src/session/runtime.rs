use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use discord_voice_service_playback::PlaybackWorker;
use discord_voice_service_playback::media::opus_queue::OpusFrameQueue;
use discord_voice_service_playback::pacer::AudioPacer;
use discord_voice_service_voice::{ConnectedVoiceSession, VoiceContext};
use tokio::sync::{Mutex, RwLock, broadcast, watch};

use super::events::{EventBus, SessionEventKind, SessionEventRecord};
use super::readiness::{
    ensure_active_voice_session, ensure_joinable_session, ensure_pauseable_track,
    ensure_resumable_track,
};
use super::state::{SessionState, Snapshot};
use super::supervisor::Command;
use crate::error::RuntimeError;

const PLAYBACK_QUEUE_CAPACITY: usize = 32;

pub struct VoiceSessionRuntime {
    state: RwLock<Snapshot>,
    events: EventBus,
    voice: Mutex<Option<ConnectedVoiceSession>>,
    media_send_gate: Mutex<()>,
    playback: Option<Mutex<PlaybackWorker>>,
    playback_epoch: AtomicU64,
    rollover_epoch: AtomicU64,
    playback_reset_pending: AtomicBool,
    playback_paused: watch::Sender<bool>,
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
            media_send_gate: Mutex::new(()),
            playback: None,
            playback_epoch: AtomicU64::new(0),
            rollover_epoch: AtomicU64::new(0),
            playback_reset_pending: AtomicBool::new(false),
            playback_paused: watch::channel(false).0,
        }
    }

    pub fn with_playback_worker(worker: PlaybackWorker) -> Self {
        Self {
            state: RwLock::new(Snapshot::default()),
            events: EventBus::new(64),
            voice: Mutex::new(None),
            media_send_gate: Mutex::new(()),
            playback: Some(Mutex::new(worker)),
            playback_epoch: AtomicU64::new(0),
            rollover_epoch: AtomicU64::new(0),
            playback_reset_pending: AtomicBool::new(false),
            playback_paused: watch::channel(false).0,
        }
    }

    pub async fn handle_command(self: &Arc<Self>, command: Command) -> Result<(), RuntimeError> {
        match command {
            Command::JoinVoice { voice } => self.join_voice(voice).await,
            Command::UpdateVoiceContext { voice } => self.update_voice_context(voice).await,
            Command::Play { video_id } => self.play(video_id).await,
            Command::Pause => self.pause().await,
            Command::Resume => self.resume().await,
            Command::Stop => self.stop().await,
            Command::LeaveVoice => self.leave_voice().await,
        }
    }

    pub async fn snapshot(&self) -> Snapshot {
        self.state.read().await.clone()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.events.subscribe()
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

    async fn play(&self, video_id: String) -> Result<(), RuntimeError> {
        let playback_epoch = self.begin_playback();
        self.play_with_epoch(video_id, playback_epoch).await
    }

    async fn play_with_epoch(
        &self,
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

        let Some(playback) = &self.playback else {
            return Ok(());
        };

        let voice_connected = self
            .voice
            .lock()
            .await
            .as_ref()
            .map(ConnectedVoiceSession::is_connected)
            .unwrap_or(false);
        if !voice_connected {
            return Ok(());
        }

        let mut queue = OpusFrameQueue::new(PLAYBACK_QUEUE_CAPACITY);
        let (selected_itag, mut source, resume_position_ms) = {
            let mut worker = playback.lock().await;
            if self.consume_playback_reset() {
                worker.reset();
            }
            let source = worker.prepare(&video_id, &mut queue).await?;
            let selected_itag = source.selected_itag();
            let resume_position_ms = source.position().sent_duration_ms();
            (selected_itag, source, resume_position_ms)
        };
        if self.playback_interrupted(playback_epoch) {
            return Ok(());
        }

        let buffering_event = {
            let mut state = self.state.write().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            state.selected_itag = Some(selected_itag);
            state.queue_depth = queue.len();
            state.position_ms = resume_position_ms;
            state.state = SessionState::Buffering;
            SessionEventRecord::from_snapshot(SessionEventKind::Buffering, &state)
        };
        tracing::debug!(
            %video_id,
            playback_epoch,
            selected_itag,
            queue_depth = queue.len(),
            resume_position_ms,
            "runtime emitting Buffering"
        );
        self.events.emit(buffering_event);

        {
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
                "runtime settling voice session before Playing"
            );
            session.wait_for_initial_dave_settle().await?;
        }

        let playing_event = {
            let mut state = self.state.write().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            state.selected_itag = Some(selected_itag);
            state.queue_depth = queue.len();
            state.position_ms = resume_position_ms;
            state.state = SessionState::Playing;
            SessionEventRecord::from_snapshot(SessionEventKind::Playing, &state)
        };
        tracing::debug!(
            %video_id,
            playback_epoch,
            selected_itag,
            queue_depth = queue.len(),
            resume_position_ms,
            "runtime emitting Playing"
        );
        self.events.emit(playing_event);

        let mut pacer = AudioPacer::new();
        let mut pause_rx = self.playback_paused.subscribe();
        let mut position_ms = resume_position_ms;
        loop {
            self.wait_while_paused(playback_epoch, &mut pause_rx, &mut pacer)
                .await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }

            let Some(frame) = queue.pop() else {
                let mut worker = playback.lock().await;
                if let Err(err) = worker.fill_queue(&mut source, &mut queue).await {
                    tracing::debug!(
                        %video_id,
                        playback_epoch,
                        position_ms,
                        queue_depth = queue.len(),
                        error = ?err,
                        "playback fill_queue failed after queue drain"
                    );
                    return Err(err.into());
                }
                if self.playback_interrupted(playback_epoch) {
                    return Ok(());
                }
                {
                    let mut state = self.state.write().await;
                    if self.playback_interrupted(playback_epoch) {
                        return Ok(());
                    }
                    state.queue_depth = queue.len();
                }

                if queue.is_empty() {
                    tracing::debug!(
                        %video_id,
                        playback_epoch,
                        position_ms,
                        "playback queue empty after refill"
                    );
                    tracing::debug!(
                        %video_id,
                        playback_epoch,
                        position_ms,
                        "playback loop exiting for natural end-of-stream"
                    );
                    break;
                }

                continue;
            };

            let frame_duration = Duration::from_nanos(
                u64::from(frame.duration_samples).saturating_mul(1_000_000_000) / 48_000,
            );
            pacer.wait_until_ready().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            self.wait_while_paused(playback_epoch, &mut pause_rx, &mut pacer)
                .await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }

            {
                let frame_duration_ms = frame.duration_ms;
                let mut retried_after_gateway_reconnect = false;
                loop {
                    let send_gate = self.media_send_gate.lock().await;
                    let mut voice = self.voice.lock().await;
                    if self.playback_interrupted(playback_epoch) {
                        return Ok(());
                    }
                    if *self.playback_paused.borrow() {
                        drop(voice);
                        drop(send_gate);
                        self.wait_while_paused(playback_epoch, &mut pause_rx, &mut pacer)
                            .await;
                        if self.playback_interrupted(playback_epoch) {
                            return Ok(());
                        }
                        continue;
                    }
                    let session = voice.as_mut().ok_or(RuntimeError::InvalidState(
                        "play requires active voice session",
                    ))?;
                    tracing::debug!(
                        %video_id,
                        playback_epoch,
                        position_ms,
                        frame_duration_ms,
                        "runtime sending audio frame"
                    );
                    let send_result = session
                        .send_audio_frame_with_duration_samples(
                            frame.data.clone(),
                            frame.duration_samples,
                        )
                        .await;
                    drop(voice);

                    match send_result {
                        Ok(()) => break,
                        Err(err)
                            if err.is_gateway_closed_during_receive()
                                && !retried_after_gateway_reconnect =>
                        {
                            tracing::info!(
                                %video_id,
                                playback_epoch,
                                position_ms,
                                frame_duration_ms,
                                error = ?err,
                                "playback reconnecting voice session after gateway close"
                            );
                            self.reconnect_voice_session_for_playback(playback_epoch)
                                .await?;
                            if self.playback_interrupted(playback_epoch) {
                                return Ok(());
                            }
                            retried_after_gateway_reconnect = true;
                        }
                        Err(err) => {
                            tracing::debug!(
                                %video_id,
                                playback_epoch,
                                position_ms,
                                frame_duration_ms,
                                error = ?err,
                                "playback send_audio_frame failed"
                            );
                            return Err(err.into());
                        }
                    }
                }
                tracing::debug!(
                    %video_id,
                    playback_epoch,
                    position_ms,
                    frame_duration_ms,
                    "runtime sent audio frame"
                );
            }
            pacer.mark_emitted(frame_duration);
            source.record_sent_packet(frame.duration_ms);
            position_ms += frame.duration_ms;

            {
                let mut worker = playback.lock().await;
                if let Err(err) = worker.fill_queue(&mut source, &mut queue).await {
                    tracing::debug!(
                        %video_id,
                        playback_epoch,
                        position_ms,
                        queue_depth = queue.len(),
                        error = ?err,
                        "playback fill_queue failed after frame send"
                    );
                    return Err(err.into());
                }
                if queue.is_empty() {
                    tracing::debug!(
                        %video_id,
                        playback_epoch,
                        position_ms,
                        "playback queue empty after refill"
                    );
                }
            }
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            {
                let mut state = self.state.write().await;
                if self.playback_interrupted(playback_epoch) {
                    return Ok(());
                }
                state.queue_depth = queue.len();
                state.position_ms = position_ms;
            }
        }

        if self.playback_interrupted(playback_epoch) {
            return Ok(());
        }

        {
            let mut voice = self.voice.lock().await;
            let session = voice.as_mut().ok_or(RuntimeError::InvalidState(
                "play requires active voice session",
            ))?;
            tracing::debug!(
                %video_id,
                playback_epoch,
                position_ms,
                "playback calling stop_audio after natural end-of-stream"
            );
            if let Err(err) = session.stop_audio().await {
                tracing::debug!(
                    %video_id,
                    playback_epoch,
                    position_ms,
                    error = ?err,
                    "playback stop_audio failed"
                );
                return Err(err.into());
            }
            tracing::debug!(
                %video_id,
                playback_epoch,
                position_ms,
                "playback stop_audio completed"
            );
        }
        if self.playback_interrupted(playback_epoch) {
            return Ok(());
        }

        let track_ended_event = {
            let mut state = self.state.write().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            state.position_ms = position_ms;
            let event = SessionEventRecord::from_snapshot(SessionEventKind::TrackEnded, &state);
            state.current_video_id = None;
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = 0;
            state.state = SessionState::VoiceReady;
            event
        };
        self.events.emit(track_ended_event);
        Ok(())
    }

    async fn pause(&self) -> Result<(), RuntimeError> {
        {
            let state = self.state.read().await;
            ensure_pauseable_track(&state)?;
        }
        self.playback_paused.send_replace(true);
        let _send_gate = self.media_send_gate.lock().await;
        tracing::debug!("runtime pausing playback and stopping speaking state");
        {
            let mut voice = self.voice.lock().await;
            if let Some(session) = voice.as_mut()
                && session.is_connected()
            {
                tracing::debug!("runtime suspending voice media for Pause");
                session.suspend_media().await?;
                tracing::debug!("runtime suspended voice media for Pause");
            }
        }

        let event = {
            let mut state = self.state.write().await;
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
            ensure_resumable_track(&state)?;
        }

        {
            let _send_gate = self.media_send_gate.lock().await;
            let reconnect_voice = {
                let mut voice = self.voice.lock().await;
                let session = voice.as_mut().ok_or(RuntimeError::InvalidState(
                    "resume requires active voice session",
                ))?;
                if session.is_connected() {
                    None
                } else if session.can_resume_gateway_after_close() {
                    tracing::debug!("runtime resuming suspended voice gateway for Resume");
                    session.resume_gateway_after_close().await?;
                    if !session.is_connected() {
                        return Err(RuntimeError::InvalidState(
                            "resume requires connected voice session",
                        ));
                    }
                    tracing::debug!("runtime resumed suspended voice gateway for Resume");
                    None
                } else {
                    Some(session.voice_context().clone())
                }
            };

            if let Some(voice_context) = reconnect_voice {
                tracing::debug!("runtime reconnecting paused voice media for Resume");
                let mut replacement = ConnectedVoiceSession::connect(voice_context).await?;
                replacement.settle_initial_dave_for_join().await?;
                if !replacement.is_connected() {
                    return Err(RuntimeError::InvalidState(
                        "resume requires connected voice session",
                    ));
                }

                let mut voice = self.voice.lock().await;
                *voice = Some(replacement);
                tracing::debug!("runtime reconnected paused voice media for Resume");
            }
        }

        let event = {
            let mut state = self.state.write().await;
            ensure_resumable_track(&state)?;
            state.state = SessionState::Playing;
            SessionEventRecord::from_snapshot(SessionEventKind::Playing, &state)
        };

        self.playback_paused.send_replace(false);
        self.events.emit(event);
        Ok(())
    }

    async fn stop(&self) -> Result<(), RuntimeError> {
        {
            let state = self.state.read().await;
            ensure_active_voice_session(&state, "stop")?;
        }
        self.invalidate_playback();
        self.defer_playback_reset();
        {
            let mut voice = self.voice.lock().await;
            if let Some(session) = voice.as_mut()
                && session.is_connected()
                && session.media_started()
            {
                session.stop_audio().await?;
            }
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

        self.invalidate_playback();
        let resume_playback_epoch = self.playback_epoch.load(Ordering::SeqCst);

        let reconnecting_event = {
            let mut state = self.state.write().await;
            state.voice_reconnecting = true;
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceReconnecting, &state)
        };
        self.events.emit(reconnecting_event);

        let replacement = match ConnectedVoiceSession::connect(new_voice.clone()).await {
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

    async fn refresh_paused_voice_context(
        &self,
        new_voice: VoiceContext,
    ) -> Result<(), RuntimeError> {
        tracing::debug!("runtime refreshing paused voice context");
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
        self.playback_paused.send_replace(false);
        epoch
    }

    fn invalidate_playback(&self) {
        self.playback_epoch.fetch_add(1, Ordering::SeqCst);
        self.playback_paused.send_replace(false);
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

    async fn wait_while_paused(
        &self,
        playback_epoch: u64,
        pause_rx: &mut watch::Receiver<bool>,
        pacer: &mut AudioPacer,
    ) {
        let mut saw_pause = false;
        loop {
            if self.playback_interrupted(playback_epoch) {
                return;
            }

            if !*pause_rx.borrow_and_update() {
                if saw_pause {
                    pacer.reset_deadline();
                }
                return;
            }

            saw_pause = true;
            tokio::select! {
                changed = pause_rx.changed() => {
                    if changed.is_err() {
                        pacer.reset_deadline();
                        return;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
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

    async fn reconnect_voice_session_for_playback(
        &self,
        playback_epoch: u64,
    ) -> Result<(), RuntimeError> {
        let voice_context = {
            let voice = self.voice.lock().await;
            voice
                .as_ref()
                .map(|session| session.voice_context().clone())
                .ok_or(RuntimeError::InvalidState(
                    "play requires active voice session",
                ))?
        };

        {
            let mut voice = self.voice.lock().await;
            if self.playback_interrupted(playback_epoch) {
                return Ok(());
            }
            let Some(session) = voice.as_mut() else {
                return Err(RuntimeError::InvalidState(
                    "play requires active voice session",
                ));
            };
            match session.resume_gateway_after_close().await {
                Ok(()) => {
                    tracing::info!(
                        playback_epoch,
                        "runtime resumed voice gateway after playback close"
                    );
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                        playback_epoch,
                        error = ?err,
                        "runtime voice gateway resume failed; falling back to full reconnect"
                    );
                }
            }
        }

        let mut replacement = ConnectedVoiceSession::connect(voice_context).await?;
        replacement.settle_initial_dave_for_join().await?;
        if self.playback_interrupted(playback_epoch) {
            return Ok(());
        }

        let mut voice = self.voice.lock().await;
        if self.playback_interrupted(playback_epoch) {
            return Ok(());
        }
        *voice = Some(replacement);
        Ok(())
    }
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
}
