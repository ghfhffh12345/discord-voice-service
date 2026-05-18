use crate::error::AppError;

#[derive(Clone, Debug)]
pub struct YtMusicClient {
    pub endpoint: String,
}

impl YtMusicClient {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    pub async fn healthcheck(&self) -> Result<(), AppError> {
        let _ = &self.endpoint;
        Ok(())
    }
}
