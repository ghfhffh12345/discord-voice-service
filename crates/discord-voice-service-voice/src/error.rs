use thiserror::Error;

#[derive(Debug, Error)]
pub enum VoiceError {
    #[error("unsupported encryption mode")]
    UnsupportedEncryptionMode,
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
}
