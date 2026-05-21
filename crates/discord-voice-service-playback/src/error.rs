use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlaybackError {
    #[error("media parse error: {0}")]
    MediaParse(&'static str),
    #[error("media parse error: {0}")]
    MediaParseDetail(String),
    #[error("unsupported format")]
    UnsupportedFormat,
    #[error("buffer is full")]
    BufferFull,
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcStatus(Box<tonic::Status>),
}

impl From<tonic::Status> for PlaybackError {
    fn from(error: tonic::Status) -> Self {
        Self::GrpcStatus(Box::new(error))
    }
}
