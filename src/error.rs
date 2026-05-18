use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("missing env var: {0}")]
    MissingEnv(&'static str),
    #[error("invalid env var: {0}")]
    InvalidEnv(&'static str),
}
