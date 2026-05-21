use std::sync::Arc;

use discord_voice_service_playback::{PlaybackWorker, YtMusicClient};
use tokio::sync::broadcast;

use super::events::SessionEventRecord;
use super::runtime::VoiceSessionRuntime;
use super::state::Snapshot;
use crate::error::RuntimeError;
pub use discord_voice_service_voice::VoiceContext;

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

    pub async fn with_ytmusic_endpoint(endpoint: String) -> Result<Self, RuntimeError> {
        let client = YtMusicClient::connect(endpoint).await?;
        Ok(Self {
            runtime: Arc::new(VoiceSessionRuntime::with_playback_worker(
                PlaybackWorker::new(client),
            )),
        })
    }

    pub async fn send(&self, command: Command) -> Result<(), RuntimeError> {
        self.runtime.handle_command(command).await
    }

    pub async fn snapshot(&self) -> Snapshot {
        self.runtime.snapshot().await
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.runtime.subscribe_events()
    }
}
