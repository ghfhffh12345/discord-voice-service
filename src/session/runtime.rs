use tokio::sync::{Mutex, RwLock, broadcast};

use crate::discord_voice::session::ConnectedVoiceSession;
use crate::error::AppError;
use crate::media::opus_queue::OpusFrameQueue;
use crate::playback::worker::PlaybackWorker;
use crate::session::events::{EventBus, SessionEventKind, SessionEventRecord};
use crate::session::readiness::{
    ensure_active_voice_session, ensure_joinable_session, ensure_pauseable_track,
    ensure_resumable_track,
};
use crate::session::state::{SessionState, Snapshot};
use crate::session::supervisor::{Command, VoiceContext};

const PLAYBACK_QUEUE_CAPACITY: usize = 32;

pub struct VoiceSessionRuntime {
    state: RwLock<Snapshot>,
    events: EventBus,
    voice: Mutex<Option<ConnectedVoiceSession>>,
    playback: Option<Mutex<PlaybackWorker>>,
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
            playback: None,
        }
    }

    pub fn with_playback_worker(worker: PlaybackWorker) -> Self {
        Self {
            state: RwLock::new(Snapshot::default()),
            events: EventBus::new(64),
            voice: Mutex::new(None),
            playback: Some(Mutex::new(worker)),
        }
    }

    pub async fn handle_command(&self, command: Command) -> Result<(), AppError> {
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

    pub async fn current_voice_context(&self) -> Option<VoiceContext> {
        self.voice
            .lock()
            .await
            .as_ref()
            .map(|session| session.voice_context().clone())
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.events.subscribe()
    }

    async fn join_voice(&self, voice: VoiceContext) -> Result<(), AppError> {
        {
            let state = self.state.read().await;
            ensure_joinable_session(&state)?;
        }

        let session = ConnectedVoiceSession::connect(voice).await?;
        let event = {
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
            let kind = if matches!(state.state, SessionState::VoiceReady) {
                SessionEventKind::VoiceReady
            } else {
                SessionEventKind::VoiceConnecting
            };
            SessionEventRecord::from_snapshot(kind, &state)
        };

        self.events.emit(event);
        Ok(())
    }

    async fn update_voice_context(&self, voice: VoiceContext) -> Result<(), AppError> {
        self.rollover_voice_context(voice).await
    }

    async fn play(&self, video_id: String) -> Result<(), AppError> {
        let resolving_event = {
            let mut state = self.state.write().await;
            ensure_active_voice_session(&state, "play")?;
            state.current_video_id = Some(video_id.clone());
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = 0;
            state.state = SessionState::ResolvingTrack;
            SessionEventRecord::from_snapshot(SessionEventKind::TrackResolving, &state)
        };
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
        let (selected_itag, mut source) = {
            let mut worker = playback.lock().await;
            let source = worker.prepare(&video_id, &mut queue).await?;
            let selected_itag = source.selected_itag();
            (selected_itag, source)
        };

        let playing_event = {
            let mut state = self.state.write().await;
            state.selected_itag = Some(selected_itag);
            state.queue_depth = queue.len();
            state.position_ms = 0;
            state.state = SessionState::Playing;
            SessionEventRecord::from_snapshot(SessionEventKind::Playing, &state)
        };
        self.events.emit(playing_event);

        let mut position_ms = 0;
        loop {
            {
                let mut voice = self.voice.lock().await;
                let session = voice
                    .as_mut()
                    .ok_or(AppError::InvalidState("play requires active voice session"))?;
                while let Some(frame) = queue.pop() {
                    position_ms += frame.duration_ms;
                    session.send_audio_frame(frame.data).await?;
                }
            }

            {
                let mut worker = playback.lock().await;
                worker.fill_queue(&mut source, &mut queue).await?;
            }

            if queue.is_empty() {
                break;
            }
        }

        let track_ended_event = {
            let mut state = self.state.write().await;
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

    async fn pause(&self) -> Result<(), AppError> {
        let event = {
            let mut state = self.state.write().await;
            ensure_pauseable_track(&state)?;
            state.state = SessionState::Paused;
            SessionEventRecord::from_snapshot(SessionEventKind::Paused, &state)
        };

        self.events.emit(event);
        Ok(())
    }

    async fn resume(&self) -> Result<(), AppError> {
        let event = {
            let mut state = self.state.write().await;
            ensure_resumable_track(&state)?;
            state.state = SessionState::Playing;
            SessionEventRecord::from_snapshot(SessionEventKind::Playing, &state)
        };

        self.events.emit(event);
        Ok(())
    }

    async fn stop(&self) -> Result<(), AppError> {
        let event = {
            let mut state = self.state.write().await;
            ensure_active_voice_session(&state, "stop")?;
            state.current_video_id = None;
            state.selected_itag = None;
            state.queue_depth = 0;
            state.position_ms = 0;
            state.state = SessionState::VoiceReady;
            SessionEventRecord::from_snapshot(SessionEventKind::Stopped, &state)
        };

        self.events.emit(event);
        Ok(())
    }

    async fn leave_voice(&self) -> Result<(), AppError> {
        *self.state.write().await = Snapshot::default();
        *self.voice.lock().await = None;
        Ok(())
    }

    async fn rollover_voice_context(&self, new_voice: VoiceContext) -> Result<(), AppError> {
        {
            let state = self.state.read().await;
            ensure_active_voice_session(&state, "update_voice_context")?;
        }

        let reconnecting_event = {
            let mut state = self.state.write().await;
            let mut current_voice = self.voice.lock().await;
            let Some(session) = current_voice.as_mut() else {
                return Err(AppError::InvalidState(
                    "update_voice_context requires active voice session",
                ));
            };

            session.rollover_mut().set_voice_reconnecting(true);
            apply_rollover_state(&mut state, session);
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceReconnecting, &state)
        };
        self.events.emit(reconnecting_event);

        let replacement = match ConnectedVoiceSession::connect(new_voice.clone()).await {
            Ok(replacement) => replacement,
            Err(err) => {
                let mut state = self.state.write().await;
                let mut current_voice = self.voice.lock().await;
                if let Some(session) = current_voice.as_mut() {
                    session.rollover_mut().set_voice_reconnecting(false);
                    apply_rollover_state(&mut state, session);
                }
                return Err(err);
            }
        };

        let reconnected_event = {
            let mut state = self.state.write().await;
            let mut current_voice = self.voice.lock().await;
            *current_voice = Some(replacement);

            let session = current_voice.as_ref().ok_or(AppError::InvalidState(
                "voice reconnect replacement missing",
            ))?;
            apply_voice_context(&mut state, session.voice_context());
            apply_rollover_state(&mut state, session);
            SessionEventRecord::from_snapshot(SessionEventKind::VoiceReady, &state)
        };
        self.events.emit(reconnected_event);

        Ok(())
    }
}

fn apply_voice_context(snapshot: &mut Snapshot, voice: &VoiceContext) {
    snapshot.guild_id = Some(voice.guild_id.clone());
    snapshot.channel_id = Some(voice.channel_id.clone());
}

fn apply_rollover_state(snapshot: &mut Snapshot, session: &ConnectedVoiceSession) {
    snapshot.recovering = session.rollover().recovering();
    snapshot.voice_reconnecting = session.rollover().voice_reconnecting();
}
