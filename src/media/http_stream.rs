use bytes::Bytes;
use reqwest::header::RANGE;

use crate::error::AppError;

use super::position::PlaybackPosition;

#[derive(Debug)]
pub struct HttpOpusStream {
    client: reqwest::Client,
    url: String,
    position: PlaybackPosition,
}

impl HttpOpusStream {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            position: PlaybackPosition::default(),
        }
    }

    pub fn position(&self) -> PlaybackPosition {
        self.position
    }

    pub fn set_resume_offset(&mut self, byte_offset: u64) {
        self.position.set_byte_offset(byte_offset);
    }

    pub async fn read_chunk(&mut self) -> Result<Option<Bytes>, AppError> {
        let range = format!("bytes={}-", self.position.byte_offset());
        let response = self
            .client
            .get(&self.url)
            .header(RANGE, range)
            .send()
            .await?
            .error_for_status()?;
        let bytes = response.bytes().await?;
        if bytes.is_empty() {
            return Ok(None);
        }

        self.position.advance_bytes(bytes.len() as u64);
        Ok(Some(bytes))
    }
}
