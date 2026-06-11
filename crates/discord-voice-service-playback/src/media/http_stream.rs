use bytes::{Buf, Bytes, BytesMut};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, RANGE};
use std::time::Duration;

use crate::error::PlaybackError;

use super::position::PlaybackPosition;

const MAX_READ_CHUNK_BYTES: usize = 64 * 1024;
const HTTP_READ_DELAY_ENV: &str = "DISCORD_VOICE_SERVICE_HTTP_READ_DELAY_MS";
const HTTP_READ_JITTER_ENV: &str = "DISCORD_VOICE_SERVICE_HTTP_READ_JITTER_MS";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpOpusStreamMetrics {
    pub response_open_count: u64,
    pub range_reopen_count: u64,
    pub bounded_chunk_count: u64,
    pub read_error_reopen_count: u64,
}

#[derive(Debug)]
pub struct HttpOpusStream {
    client: reqwest::Client,
    url: String,
    response: Option<reqwest::Response>,
    response_start: u64,
    response_end: Option<u64>,
    resource_end: Option<u64>,
    pending: BytesMut,
    position: PlaybackPosition,
    metrics: HttpOpusStreamMetrics,
    read_stress: HttpReadStressProfile,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct HttpReadStressProfile {
    base_delay_ms: u64,
    jitter_ms: u64,
}

impl HttpReadStressProfile {
    fn from_env() -> Self {
        Self {
            base_delay_ms: read_u64_env(HTTP_READ_DELAY_ENV),
            jitter_ms: read_u64_env(HTTP_READ_JITTER_ENV),
        }
    }

    fn delay_for_chunk(self, chunk_index: u64) -> Duration {
        let jitter_ms = if self.jitter_ms == 0 {
            0
        } else {
            pseudo_jitter_ms(chunk_index, self.jitter_ms)
        };
        Duration::from_millis(self.base_delay_ms.saturating_add(jitter_ms))
    }
}

impl HttpOpusStream {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            response: None,
            response_start: 0,
            response_end: None,
            resource_end: None,
            pending: BytesMut::new(),
            position: PlaybackPosition::default(),
            metrics: HttpOpusStreamMetrics::default(),
            read_stress: HttpReadStressProfile::from_env(),
        }
    }

    pub fn position(&self) -> PlaybackPosition {
        self.position
    }

    pub fn metrics(&self) -> HttpOpusStreamMetrics {
        self.metrics
    }

    pub fn set_resume_offset(&mut self, byte_offset: u64) {
        self.position.set_byte_offset(byte_offset);
        self.response = None;
        self.response_start = byte_offset;
        self.response_end = None;
        self.resource_end = None;
        self.pending.clear();
    }

    pub async fn read_chunk(&mut self) -> Result<Option<Bytes>, PlaybackError> {
        loop {
            if let Some(chunk) = self.take_pending_chunk() {
                self.apply_read_stress().await;
                return Ok(Some(chunk));
            }

            if self.response.is_none() {
                match self.open_response().await? {
                    Some(response) => {
                        let response_start = self.position.byte_offset();
                        let extent = response_extent(&response, response_start)?;
                        self.response_start = response_start;
                        self.response_end = extent.response_end;
                        self.resource_end = extent.resource_end;
                        self.response = Some(response);
                    }
                    None => return Ok(None),
                }
            }

            let Some(response) = self.response.as_mut() else {
                continue;
            };

            match response.chunk().await {
                Ok(Some(chunk)) => self.pending.extend_from_slice(&chunk),
                Ok(None) => {
                    self.response = None;
                    if self.should_reopen_after_response_eof()? {
                        self.response_end = None;
                        continue;
                    }
                    self.response_end = None;
                    self.resource_end = None;
                    return Ok(None);
                }
                Err(_err) if !self.pending.is_empty() => {
                    self.response = None;
                    self.response_end = None;
                }
                Err(_err) if self.position.byte_offset() > self.response_start => {
                    self.response = None;
                    self.response_end = None;
                    self.metrics.read_error_reopen_count =
                        self.metrics.read_error_reopen_count.saturating_add(1);
                }
                Err(err) => {
                    self.response = None;
                    self.response_end = None;
                    return Err(err.into());
                }
            }
        }
    }

    fn should_reopen_after_response_eof(&self) -> Result<bool, PlaybackError> {
        let position = self.position.byte_offset();
        let ended_before_response_end = self
            .response_end
            .is_some_and(|response_end| position < response_end);
        let ended_before_resource_end = self
            .resource_end
            .is_some_and(|resource_end| position < resource_end);
        if !ended_before_response_end && !ended_before_resource_end {
            return Ok(false);
        }
        if position <= self.response_start {
            return Err(PlaybackError::MediaParseDetail(
                "playback source response ended before advancing".into(),
            ));
        }
        Ok(true)
    }

    async fn open_response(&mut self) -> Result<Option<reqwest::Response>, PlaybackError> {
        let expected_start = self.position.byte_offset();
        let mut request = self.client.get(&self.url);
        if expected_start > 0 {
            request = request.header(RANGE, format!("bytes={expected_start}-"));
            self.metrics.range_reopen_count = self.metrics.range_reopen_count.saturating_add(1);
        }
        self.metrics.response_open_count = self.metrics.response_open_count.saturating_add(1);

        let response = request.send().await?;
        if expected_start > 0 && response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(None);
        }

        let response = response.error_for_status()?;
        validate_resume_response(&response, expected_start)?;
        Ok(Some(response))
    }

    fn take_pending_chunk(&mut self) -> Option<Bytes> {
        if self.pending.is_empty() {
            return None;
        }

        let chunk_len = self.pending.len().min(MAX_READ_CHUNK_BYTES);
        let bytes = self.pending.copy_to_bytes(chunk_len);
        if bytes.is_empty() {
            return None;
        }
        self.metrics.bounded_chunk_count = self.metrics.bounded_chunk_count.saturating_add(1);
        self.position.advance_bytes(bytes.len() as u64);
        Some(bytes)
    }

    async fn apply_read_stress(&self) {
        let delay = self
            .read_stress
            .delay_for_chunk(self.metrics.bounded_chunk_count);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

fn read_u64_env(name: &str) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

fn pseudo_jitter_ms(chunk_index: u64, max_jitter_ms: u64) -> u64 {
    let mut value = chunk_index.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    value ^= value >> 33;
    value = value.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    value ^= value >> 29;
    value % max_jitter_ms.saturating_add(1)
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ResponseExtent {
    response_end: Option<u64>,
    resource_end: Option<u64>,
}

fn response_extent(
    response: &reqwest::Response,
    response_start: u64,
) -> Result<ResponseExtent, PlaybackError> {
    let content_length_end = response
        .content_length()
        .map(|length| response_start.saturating_add(length));

    let Some(content_range) = response.headers().get(CONTENT_RANGE) else {
        return Ok(ResponseExtent {
            response_end: content_length_end,
            resource_end: None,
        });
    };
    let content_range = content_range.to_str().map_err(|_| {
        PlaybackError::MediaParseDetail(
            "range response had invalid Content-Range header".to_owned(),
        )
    })?;
    let Some(parsed) = parse_content_range(content_range) else {
        return Err(PlaybackError::MediaParseDetail(format!(
            "range response had unsupported Content-Range header: {content_range}"
        )));
    };
    if parsed.start != response_start {
        return Err(PlaybackError::MediaParseDetail(format!(
            "range response started at byte {}, expected {response_start}",
            parsed.start
        )));
    }

    Ok(ResponseExtent {
        response_end: Some(parsed.end.saturating_add(1)),
        resource_end: parsed.complete_length,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedContentRange {
    start: u64,
    end: u64,
    complete_length: Option<u64>,
}

fn parse_content_range(value: &str) -> Option<ParsedContentRange> {
    let value = value.strip_prefix("bytes ")?;
    let (range, complete_length) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    let complete_length = match complete_length {
        "*" => None,
        value => Some(value.parse().ok()?),
    };
    Some(ParsedContentRange {
        start,
        end,
        complete_length,
    })
}
