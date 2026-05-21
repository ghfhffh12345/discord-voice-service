use bytes::{Bytes, BytesMut};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, RANGE};

use crate::error::PlaybackError;

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

    pub async fn read_chunk(&mut self) -> Result<Option<Bytes>, PlaybackError> {
        let expected_start = self.position.byte_offset();
        let range = format!("bytes={expected_start}-");
        let mut response = self
            .client
            .get(&self.url)
            .header(RANGE, range)
            .send()
            .await?;
        if expected_start > 0 && response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(None);
        }
        response = response.error_for_status()?;
        validate_resume_response(&response, expected_start)?;
        let mut body = BytesMut::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => body.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(_err) if !body.is_empty() => break,
                Err(err) => return Err(err.into()),
            }
        }

        let bytes = body.freeze();
        if bytes.is_empty() {
            return Ok(None);
        }

        self.position.advance_bytes(bytes.len() as u64);
        Ok(Some(bytes))
    }
}

fn validate_resume_response(
    response: &reqwest::Response,
    expected_start: u64,
) -> Result<(), PlaybackError> {
    if expected_start == 0 {
        return Ok(());
    }

    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(PlaybackError::MediaParseDetail(format!(
            "range request was not honored: expected 206 for resume at byte {expected_start}, got {}",
            response.status()
        )));
    }

    let Some(content_range) = response.headers().get(CONTENT_RANGE) else {
        return Err(PlaybackError::MediaParseDetail(format!(
            "range request was not honored: missing Content-Range for resume at byte {expected_start}"
        )));
    };

    let content_range = content_range.to_str().map_err(|_| {
        PlaybackError::MediaParseDetail(
            "range request was not honored: invalid Content-Range header".into(),
        )
    })?;

    if !content_range.starts_with(&format!("bytes {expected_start}-")) {
        return Err(PlaybackError::MediaParseDetail(format!(
            "range request was not honored: expected Content-Range to start at byte {expected_start}, got {content_range}"
        )));
    }

    Ok(())
}
