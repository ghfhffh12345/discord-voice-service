use bytes::{Bytes, BytesMut};

use crate::error::AppError;

#[derive(Default)]
pub struct WebmOpusDemux {
    pending: BytesMut,
}

impl WebmOpusDemux {
    pub fn push_bytes(&mut self, chunk: Bytes) {
        self.pending.extend_from_slice(&chunk);
    }

    pub fn drain_packets(&mut self) -> Result<Vec<Bytes>, AppError> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }

        Ok(vec![self.pending.split().freeze()])
    }
}

pub fn extract_mock_opus_packets(_input: &Bytes) -> Result<Vec<Bytes>, AppError> {
    Ok(vec![
        Bytes::from_static(b"opus-0"),
        Bytes::from_static(b"opus-1"),
        Bytes::from_static(b"opus-2"),
    ])
}
