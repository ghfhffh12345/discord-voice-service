#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlaybackPosition {
    byte_offset: u64,
    timestamp_ms: u64,
}

impl PlaybackPosition {
    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    pub fn set_byte_offset(&mut self, byte_offset: u64) {
        self.byte_offset = byte_offset;
    }

    pub fn advance_bytes(&mut self, bytes: u64) {
        self.byte_offset = self.byte_offset.saturating_add(bytes);
    }

    pub fn set_timestamp_ms(&mut self, timestamp_ms: u64) {
        self.timestamp_ms = timestamp_ms;
    }

    pub fn advance_timestamp_ms(&mut self, duration_ms: u64) {
        self.timestamp_ms = self.timestamp_ms.saturating_add(duration_ms);
    }
}
