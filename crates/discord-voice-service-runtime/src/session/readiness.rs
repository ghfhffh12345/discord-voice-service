use std::sync::{Arc, OnceLock};

use tokio::sync::RwLock;

use super::state::{SessionState, Snapshot};
use crate::error::RuntimeError;

static GLOBAL_READINESS: OnceLock<Arc<Readiness>> = OnceLock::new();

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    pub runtime_booted: bool,
    pub ytmusic_healthy: bool,
}

impl ReadinessSnapshot {
    pub fn is_ready(self) -> bool {
        self.runtime_booted && self.ytmusic_healthy
    }
}

#[derive(Debug, Default)]
pub struct Readiness {
    state: RwLock<ReadinessSnapshot>,
}

impl Readiness {
    pub fn global() -> Arc<Self> {
        GLOBAL_READINESS
            .get_or_init(|| Arc::new(Self::default()))
            .clone()
    }

    pub async fn snapshot(&self) -> ReadinessSnapshot {
        *self.state.read().await
    }

    pub async fn is_ready(&self) -> bool {
        self.snapshot().await.is_ready()
    }

    pub async fn mark_runtime_booted(&self) {
        let snapshot = {
            let mut state = self.state.write().await;
            state.runtime_booted = true;
            *state
        };
        crate::observability::global().record_readiness(snapshot);
    }

    pub async fn mark_ytmusic_healthy(&self) {
        let snapshot = {
            let mut state = self.state.write().await;
            state.ytmusic_healthy = true;
            *state
        };
        crate::observability::global().record_readiness(snapshot);
    }

    pub async fn mark_ytmusic_unhealthy(&self) {
        let snapshot = {
            let mut state = self.state.write().await;
            state.ytmusic_healthy = false;
            *state
        };
        crate::observability::global().record_readiness(snapshot);
    }
}

pub fn ensure_joinable_session(snapshot: &Snapshot) -> Result<(), RuntimeError> {
    if matches!(snapshot.state, SessionState::Idle)
        && snapshot.guild_id.is_none()
        && snapshot.channel_id.is_none()
        && snapshot.current_video_id.is_none()
    {
        Ok(())
    } else {
        Err(RuntimeError::InvalidState(
            "join_voice requires an idle session",
        ))
    }
}

pub fn ensure_active_voice_session(
    snapshot: &Snapshot,
    action: &'static str,
) -> Result<(), RuntimeError> {
    if snapshot.guild_id.is_some() && snapshot.channel_id.is_some() {
        Ok(())
    } else {
        Err(RuntimeError::InvalidState(match action {
            "play" => "play requires active voice session",
            "pause" => "pause requires active voice session",
            "resume" => "resume requires active voice session",
            "stop" => "stop requires active voice session",
            "update_voice_context" => "update_voice_context requires active voice session",
            _ => "command requires active voice session",
        }))
    }
}

pub fn ensure_track_loaded(snapshot: &Snapshot, action: &'static str) -> Result<(), RuntimeError> {
    ensure_active_voice_session(snapshot, action)?;

    if snapshot.current_video_id.is_some() {
        Ok(())
    } else {
        Err(RuntimeError::InvalidState(match action {
            "pause" => "pause requires an active track",
            "resume" => "resume requires an active track",
            _ => "command requires an active track",
        }))
    }
}

pub fn ensure_pauseable_track(snapshot: &Snapshot) -> Result<(), RuntimeError> {
    ensure_track_loaded(snapshot, "pause")?;

    if matches!(snapshot.state, SessionState::Playing) {
        Ok(())
    } else {
        Err(RuntimeError::InvalidState("pause requires a playing track"))
    }
}

pub fn ensure_resumable_track(snapshot: &Snapshot) -> Result<(), RuntimeError> {
    ensure_track_loaded(snapshot, "resume")?;

    if matches!(snapshot.state, SessionState::Paused | SessionState::Playing) {
        Ok(())
    } else {
        Err(RuntimeError::InvalidState(
            "resume requires a paused or playing track",
        ))
    }
}
