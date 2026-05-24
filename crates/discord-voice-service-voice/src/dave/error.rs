use std::num::ParseIntError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaveError {
    #[error("DAVE session has not been initialized")]
    NotInitialized,
    #[error("DAVE string user id is invalid: {0}")]
    InvalidUserId(#[from] ParseIntError),
    #[error("DAVE returned no bytes for {0}")]
    EmptyOutput(&'static str),
    #[error("DAVE MLS failure during {context}: {mls_source}: {reason}")]
    MlsFailure {
        context: &'static str,
        mls_source: String,
        reason: String,
    },
    #[error("DAVE commit/welcome payload is malformed")]
    MalformedCommitWelcome,
    #[error("DAVE operation failed during {context}: {reason}")]
    Operation {
        context: &'static str,
        reason: String,
    },
}

impl DaveError {
    pub(crate) fn operation(context: &'static str, source: impl std::fmt::Display) -> Self {
        Self::Operation {
            context,
            reason: source.to_string(),
        }
    }

    pub(crate) fn mls_failure(
        context: &'static str,
        mls_source: &'static str,
        reason: impl std::fmt::Display,
    ) -> Self {
        Self::MlsFailure {
            context,
            mls_source: mls_source.to_owned(),
            reason: reason.to_string(),
        }
    }
}
