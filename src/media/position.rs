use std::sync::{Arc, Mutex};

use super::webm_demux::DemuxedPacket;

pub type SharedPlaybackPosition = Arc<Mutex<PlaybackPosition>>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlaybackPosition {
    byte_offset: u64,
    timestamp_ms: u64,
    sent_duration_ms: u64,
}

impl PlaybackPosition {
    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    pub fn sent_duration_ms(&self) -> u64 {
        self.sent_duration_ms
    }

    pub fn snapshot(&self) -> Self {
        *self
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

    pub fn record_buffered(&mut self, packet: &DemuxedPacket) {
        let buffered_until = packet.timestamp_ms.saturating_add(packet.duration_ms);
        self.timestamp_ms = self.timestamp_ms.max(buffered_until);
    }

    pub fn record_sent_packet(&mut self, duration_ms: u64) {
        self.sent_duration_ms = self.sent_duration_ms.saturating_add(duration_ms);
    }
}

pub fn shared_playback_position(position: PlaybackPosition) -> SharedPlaybackPosition {
    Arc::new(Mutex::new(position))
}
