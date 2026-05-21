use discord_voice_service_playback::PlaybackError;
use discord_voice_service_voice::VoiceError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    Playback(#[from] PlaybackError),
    #[error(transparent)]
    Voice(#[from] VoiceError),
}
