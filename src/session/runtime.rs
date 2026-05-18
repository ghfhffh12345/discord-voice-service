use tokio::sync::{RwLock, broadcast};

use crate::error::AppError;
use crate::session::events::{EventBus, SessionEventKind, SessionEventRecord};
use crate::session::readiness::{
    ensure_active_voice_session, ensure_joinable_session, ensure_pauseable_track,
    ensure_resumable_track,
};
use crate::session::state::{SessionState, Snapshot};
use crate::session::supervisor::{Command, VoiceContext};

pub struct VoiceSessionRuntime {
    snapshot: RwLock<Snapshot>,
    events: EventBus,
}

impl VoiceSessionRuntime {
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(Snapshot::default()),
            events: EventBus::new(64),
        }
    }

    pub async fn handle_command(&self, command: Command) -> Result<(), AppError> {
        let event = {
            let mut snapshot = self.snapshot.write().await;
            match command {
                Command::JoinVoice { voice } => {
                    ensure_joinable_session(&snapshot)?;
                    apply_voice_context(&mut snapshot, voice);
                    snapshot.current_video_id = None;
                    snapshot.selected_itag = None;
                    snapshot.queue_depth = 0;
                    snapshot.position_ms = 0;
                    snapshot.recovering = false;
                    snapshot.voice_reconnecting = false;
                    snapshot.last_reason = None;
                    snapshot.state = SessionState::VoiceReady;
                    Some(SessionEventRecord::new(SessionEventKind::VoiceConnecting))
                }
                Command::UpdateVoiceContext { voice } => {
                    ensure_active_voice_session(&snapshot, "update_voice_context")?;
                    apply_voice_context(&mut snapshot, voice);
                    None
                }
                Command::Play { video_id } => {
                    ensure_active_voice_session(&snapshot, "play")?;
                    snapshot.current_video_id = Some(video_id);
                    snapshot.selected_itag = None;
                    snapshot.position_ms = 0;
                    snapshot.state = SessionState::ResolvingTrack;
                    Some(SessionEventRecord::new(SessionEventKind::TrackResolving))
                }
                Command::Pause => {
                    ensure_pauseable_track(&snapshot)?;
                    snapshot.state = SessionState::Paused;
                    Some(SessionEventRecord::new(SessionEventKind::Paused))
                }
                Command::Resume => {
                    ensure_resumable_track(&snapshot)?;
                    snapshot.state = SessionState::Playing;
                    Some(SessionEventRecord::new(SessionEventKind::Playing))
                }
                Command::Stop => {
                    ensure_active_voice_session(&snapshot, "stop")?;
                    snapshot.current_video_id = None;
                    snapshot.selected_itag = None;
                    snapshot.queue_depth = 0;
                    snapshot.position_ms = 0;
                    snapshot.state = SessionState::VoiceReady;
                    Some(SessionEventRecord::new(SessionEventKind::Stopped))
                }
                Command::LeaveVoice => {
                    *snapshot = Snapshot::default();
                    None
                }
            }
        };

        if let Some(event) = event {
            self.events.emit(event);
        }

        Ok(())
    }

    pub async fn snapshot(&self) -> Snapshot {
        self.snapshot.read().await.clone()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.events.subscribe()
    }
}

fn apply_voice_context(snapshot: &mut Snapshot, voice: VoiceContext) {
    snapshot.guild_id = Some(voice.guild_id);
    snapshot.channel_id = Some(voice.channel_id);
}
