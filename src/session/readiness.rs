use crate::error::AppError;
use crate::session::state::{SessionState, Snapshot};

pub fn ensure_joinable_session(snapshot: &Snapshot) -> Result<(), AppError> {
    if matches!(snapshot.state, SessionState::Idle)
        && snapshot.guild_id.is_none()
        && snapshot.channel_id.is_none()
        && snapshot.current_video_id.is_none()
    {
        Ok(())
    } else {
        Err(AppError::InvalidState(
            "join_voice requires an idle session",
        ))
    }
}

pub fn ensure_active_voice_session(
    snapshot: &Snapshot,
    action: &'static str,
) -> Result<(), AppError> {
    if snapshot.guild_id.is_some() && snapshot.channel_id.is_some() {
        Ok(())
    } else {
        Err(AppError::InvalidState(match action {
            "play" => "play requires active voice session",
            "pause" => "pause requires active voice session",
            "resume" => "resume requires active voice session",
            "stop" => "stop requires active voice session",
            "update_voice_context" => "update_voice_context requires active voice session",
            _ => "command requires active voice session",
        }))
    }
}

pub fn ensure_track_loaded(snapshot: &Snapshot, action: &'static str) -> Result<(), AppError> {
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

pub fn ensure_pauseable_track(snapshot: &Snapshot) -> Result<(), AppError> {
    ensure_track_loaded(snapshot, "pause")?;

    if matches!(snapshot.state, SessionState::Playing) {
        Ok(())
    } else {
        Err(AppError::InvalidState("pause requires a playing track"))
    }
}

pub fn ensure_resumable_track(snapshot: &Snapshot) -> Result<(), AppError> {
    ensure_track_loaded(snapshot, "resume")?;

    if matches!(snapshot.state, SessionState::Paused | SessionState::Playing) {
        Ok(())
    } else {
        Err(AppError::InvalidState(
            "resume requires a paused or playing track",
        ))
    }
}
