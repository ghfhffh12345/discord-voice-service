use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("missing env var: {0}")]
    MissingEnv(&'static str),
    #[error("invalid env var: {0}")]
    InvalidEnv(&'static str),
    #[error("media parse error: {0}")]
    MediaParse(&'static str),
    #[error("unsupported format")]
    UnsupportedFormat,
    #[error("buffer is full")]
    BufferFull,
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),
}
