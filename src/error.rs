use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("missing env var: {0}")]
    MissingEnv(&'static str),
    #[error("invalid env var: {0}")]
    InvalidEnv(&'static str),
    #[error("media parse error: {0}")]
    MediaParse(&'static str),
    #[error("media parse error: {0}")]
    MediaParseDetail(String),
    #[error("unsupported format")]
    UnsupportedFormat,
    #[error("buffer is full")]
    BufferFull,
    #[error("unsupported encryption mode")]
    UnsupportedEncryptionMode,
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),
}
