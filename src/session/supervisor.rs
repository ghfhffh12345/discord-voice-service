use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::AppError;
use crate::session::state::{SessionState, Snapshot};

#[derive(Debug, Clone)]
pub enum Command {
    JoinVoice {
        guild_id: String,
        channel_id: String,
        session_id: String,
        endpoint: String,
        token: String,
    },
    Play {
        video_id: String,
    },
    Pause,
    Resume,
    Stop,
    LeaveVoice,
}

#[derive(Clone, Default)]
pub struct Supervisor {
    snapshot: Arc<RwLock<Snapshot>>,
}

impl Supervisor {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn send(&self, command: Command) -> Result<(), AppError> {
        let mut snapshot = self.snapshot.write().await;
        match command {
            Command::JoinVoice {
                guild_id,
                channel_id,
                ..
            } => {
                snapshot.guild_id = Some(guild_id);
                snapshot.channel_id = Some(channel_id);
                snapshot.current_video_id = None;
                snapshot.state = SessionState::VoiceReady;
            }
            Command::Play { video_id } => {
                ensure_active_voice_session(&snapshot, "play")?;
                snapshot.current_video_id = Some(video_id);
                snapshot.state = SessionState::ResolvingTrack;
            }
            Command::Pause => {
                ensure_track_loaded(&snapshot, "pause")?;
                snapshot.state = SessionState::Paused;
            }
            Command::Resume => {
                ensure_resumable_track(&snapshot)?;
                snapshot.state = SessionState::Playing;
            }
            Command::Stop => {
                ensure_active_voice_session(&snapshot, "stop")?;
                snapshot.current_video_id = None;
                snapshot.state = SessionState::VoiceReady;
            }
            Command::LeaveVoice => *snapshot = Snapshot::default(),
        }
        Ok(())
    }

    pub async fn snapshot(&self) -> Snapshot {
        self.snapshot.read().await.clone()
    }
}

fn ensure_active_voice_session(snapshot: &Snapshot, action: &'static str) -> Result<(), AppError> {
    if snapshot.guild_id.is_some() && snapshot.channel_id.is_some() {
        Ok(())
    } else {
        Err(AppError::InvalidState(match action {
            "play" => "play requires active voice session",
            "pause" => "pause requires active voice session",
            "resume" => "resume requires active voice session",
            "stop" => "stop requires active voice session",
            _ => "command requires active voice session",
        }))
    }
}

fn ensure_track_loaded(snapshot: &Snapshot, action: &'static str) -> Result<(), AppError> {
    ensure_active_voice_session(snapshot, action)?;

    if snapshot.current_video_id.is_some() {
        Ok(())
    } else {
        Err(AppError::InvalidState(match action {
            "pause" => "pause requires an active track",
            "resume" => "resume requires an active track",
            _ => "command requires an active track",
        }))
    }
}

fn ensure_resumable_track(snapshot: &Snapshot) -> Result<(), AppError> {
    ensure_track_loaded(snapshot, "resume")?;

    if matches!(snapshot.state, SessionState::Paused | SessionState::Playing) {
        Ok(())
    } else {
        Err(AppError::InvalidState(
            "resume requires a paused or playing track",
        ))
    }
}
