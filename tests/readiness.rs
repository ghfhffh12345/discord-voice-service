use std::sync::Arc;

use discord_voice_service::session::readiness::Readiness;
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};

#[tokio::test]
async fn readiness_depends_on_runtime_boot_and_ytmusic_reachability_not_playback() {
    let harness = ReadinessHarness::spawn().await;
    assert!(!harness.ready().await);

    harness.mark_runtime_booted().await;
    harness.mark_ytmusic_healthy().await;
    assert!(harness.ready().await);

    harness.mark_idle().await;
    assert!(harness.ready().await);
}

struct ReadinessHarness {
    readiness: Arc<Readiness>,
    supervisor: Supervisor,
}

impl ReadinessHarness {
    async fn spawn() -> Self {
        Self {
            readiness: Arc::new(Readiness::default()),
            supervisor: Supervisor::new(),
        }
    }

    async fn ready(&self) -> bool {
        self.readiness.is_ready().await
    }

    async fn mark_runtime_booted(&self) {
        self.readiness.mark_runtime_booted().await;
    }

    async fn mark_ytmusic_healthy(&self) {
        self.readiness.mark_ytmusic_healthy().await;
    }

    async fn mark_idle(&self) {
        self.supervisor
            .send(Command::JoinVoice {
                voice: VoiceContext {
                    guild_id: "1".into(),
                    channel_id: "2".into(),
                    user_id: "user-1".into(),
                    session_id: "3".into(),
                    endpoint: "voice-placeholder".into(),
                    token: "token".into(),
                },
            })
            .await
            .unwrap();
        self.supervisor.send(Command::LeaveVoice).await.unwrap();
    }
}
