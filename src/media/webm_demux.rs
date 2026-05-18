use bytes::Bytes;

use crate::error::AppError;

pub fn extract_mock_opus_packets(_input: &Bytes) -> Result<Vec<Bytes>, AppError> {
    Ok(vec![
        Bytes::from_static(b"opus-0"),
        Bytes::from_static(b"opus-1"),
        Bytes::from_static(b"opus-2"),
    ])
}
