use std::sync::Arc;

use tokio::sync::broadcast;

use crate::error::AppError;
use crate::playback::worker::PlaybackWorker;
use crate::session::events::SessionEventRecord;
use crate::session::runtime::VoiceSessionRuntime;
use crate::session::state::Snapshot;
use crate::ytmusic::client::YtMusicClient;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceContext {
    pub guild_id: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_id: String,
    pub endpoint: String,
    pub token: String,
}

#[derive(Debug, Clone)]
pub enum Command {
    JoinVoice { voice: VoiceContext },
    UpdateVoiceContext { voice: VoiceContext },
    Play { video_id: String },
    Pause,
    Resume,
    Stop,
    LeaveVoice,
}

#[derive(Clone)]
pub struct Supervisor {
    runtime: Arc<VoiceSessionRuntime>,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            runtime: Arc::new(VoiceSessionRuntime::new()),
        }
    }

    pub async fn with_ytmusic_endpoint(endpoint: String) -> Result<Self, AppError> {
        let client = YtMusicClient::connect(endpoint).await?;
        Ok(Self {
            runtime: Arc::new(VoiceSessionRuntime::with_playback_worker(
                PlaybackWorker::new(client),
            )),
        })
    }

    pub async fn send(&self, command: Command) -> Result<(), AppError> {
        self.runtime.handle_command(command).await
    }

    pub async fn snapshot(&self) -> Snapshot {
        self.runtime.snapshot().await
    }

    pub async fn current_voice_context(&self) -> Option<VoiceContext> {
        self.runtime.current_voice_context().await
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.runtime.subscribe_events()
    }
}
