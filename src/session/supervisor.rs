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
                if snapshot.guild_id.is_none() {
                    return Err(AppError::InvalidState("play requires active voice session"));
                }
                snapshot.current_video_id = Some(video_id);
                snapshot.state = SessionState::ResolvingTrack;
            }
            Command::Pause => snapshot.state = SessionState::Paused,
            Command::Resume => snapshot.state = SessionState::Playing,
            Command::Stop => {
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
