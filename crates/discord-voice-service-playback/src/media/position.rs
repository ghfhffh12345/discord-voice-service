use std::sync::{Arc, Mutex};

use super::opus_queue::duration_ms_from_samples;
use super::webm_demux::DemuxedPacket;

pub type SharedPlaybackPosition = Arc<Mutex<PlaybackPosition>>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlaybackPosition {
    byte_offset: u64,
    timestamp_ms: u64,
    timestamp_samples: u64,
    sent_duration_samples: u64,
}

impl PlaybackPosition {
    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    pub fn timestamp_samples(&self) -> u64 {
        self.timestamp_samples
    }

    pub fn sent_duration_ms(&self) -> u64 {
        duration_ms_from_samples(self.sent_duration_samples)
    }

    pub fn sent_duration_samples(&self) -> u64 {
        self.sent_duration_samples
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
        self.timestamp_samples = self.timestamp_samples.max(timestamp_ms.saturating_mul(48));
    }

    pub fn advance_timestamp_ms(&mut self, duration_ms: u64) {
        self.timestamp_ms = self.timestamp_ms.saturating_add(duration_ms);
        self.timestamp_samples = self
            .timestamp_samples
            .max(self.timestamp_ms.saturating_mul(48));
    }

    pub fn record_buffered(&mut self, packet: &DemuxedPacket) {
        let buffered_until_samples = packet
            .timestamp_samples
            .saturating_add(u64::from(packet.duration_samples));
        self.timestamp_samples = self.timestamp_samples.max(buffered_until_samples);
        self.timestamp_ms = self
            .timestamp_ms
            .max(duration_ms_from_samples(self.timestamp_samples));
    }

    pub fn record_sent_packet_samples(&mut self, duration_samples: u32) {
        self.record_sent_duration_samples(u64::from(duration_samples));
    }

    pub fn record_sent_duration_samples(&mut self, duration_samples: u64) {
        self.sent_duration_samples = self.sent_duration_samples.saturating_add(duration_samples);
    }

    pub fn set_sent_duration_samples(&mut self, sent_duration_samples: u64) {
        self.sent_duration_samples = sent_duration_samples;
    }
}

pub fn shared_playback_position(position: PlaybackPosition) -> SharedPlaybackPosition {
    Arc::new(Mutex::new(position))
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;

    #[test]
    fn position_tracks_fractional_packet_samples_without_ms_drift() {
        let mut position = PlaybackPosition::default();
        let first = DemuxedPacket {
            data: Bytes::from_static(b"a"),
            timestamp_ms: 0,
            timestamp_samples: 0,
            duration_ms: 2,
            duration_samples: 120,
        };
        let second = DemuxedPacket {
            data: Bytes::from_static(b"b"),
            timestamp_ms: 2,
            timestamp_samples: 120,
            duration_ms: 2,
            duration_samples: 120,
        };

        position.record_buffered(&first);
        position.record_buffered(&second);
        position.record_sent_packet_samples(first.duration_samples);
        position.record_sent_packet_samples(second.duration_samples);

        assert_eq!(position.timestamp_samples, 240);
        assert_eq!(position.timestamp_ms(), 5);
        assert_eq!(position.sent_duration_samples(), 240);
        assert_eq!(position.sent_duration_ms(), 5);
    }
}
