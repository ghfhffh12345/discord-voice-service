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
}
