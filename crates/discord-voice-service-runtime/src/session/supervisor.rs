use std::sync::Arc;
use std::time::Duration;

use discord_voice_service_playback::{PlaybackWorker, YtMusicClient};
use tokio::sync::broadcast;

use super::events::SessionEventRecord;
use super::metrics::PlaybackStabilitySnapshot;
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

    pub async fn playback_metrics(&self) -> Option<PlaybackStabilitySnapshot> {
        self.runtime.playback_metrics().await
    }

    #[doc(hidden)]
    #[cfg(debug_assertions)]
    pub fn set_live_media_send_delay_for_tests<F>(&self, delay_for_packet: F)
    where
        F: Fn(u64) -> Option<Duration> + Send + Sync + 'static,
    {
        self.runtime
            .set_live_media_send_delay_for_tests(delay_for_packet);
    }

    #[doc(hidden)]
    #[cfg(debug_assertions)]
    pub fn clear_live_media_send_delay_for_tests(&self) {
        self.runtime.clear_live_media_send_delay_for_tests();
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.runtime.subscribe_events()
    }
}
