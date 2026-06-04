use std::collections::VecDeque;

use crate::media::http_stream::HttpOpusStream;
use crate::media::http_stream::HttpOpusStreamMetrics;
use crate::media::position::{PlaybackPosition, SharedPlaybackPosition};
use crate::media::webm_demux::{DemuxedPacket, WebmOpusDemux};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedPlaybackSource {
    pub selected_itag: u32,
    pub playable_url: String,
    pub approx_duration_ms: Option<u64>,
}

impl ResolvedPlaybackSource {
    pub(crate) fn from_parts(
        selected_itag: u32,
        playable_url: String,
        approx_duration_ms: Option<u64>,
    ) -> Self {
        Self {
            selected_itag,
            playable_url,
            approx_duration_ms,
        }
    }
}

pub struct PlaybackSource {
    resolved: ResolvedPlaybackSource,
    stream: HttpOpusStream,
    demux: WebmOpusDemux,
    pending_packets: VecDeque<DemuxedPacket>,
    position: SharedPlaybackPosition,
}

impl PlaybackSource {
    pub fn new(
        resolved: ResolvedPlaybackSource,
        stream: HttpOpusStream,
        demux: WebmOpusDemux,
        pending_packets: VecDeque<DemuxedPacket>,
        position: SharedPlaybackPosition,
    ) -> Self {
        Self {
            resolved,
            stream,
            demux,
            pending_packets,
            position,
        }
    }

    pub fn selected_itag(&self) -> u32 {
        self.resolved.selected_itag
    }

    pub fn resolved(&self) -> &ResolvedPlaybackSource {
        &self.resolved
    }

    pub fn playable_url(&self) -> &str {
        &self.resolved.playable_url
    }

    pub fn position(&self) -> PlaybackPosition {
        self.live_position()
    }

    pub fn shared_position(&self) -> SharedPlaybackPosition {
        self.position.clone()
    }

    pub fn record_sent_packet(&mut self, duration_ms: u64) {
        self.sync_position();
        self.position
            .lock()
            .unwrap()
            .record_sent_packet(duration_ms);
    }

    pub fn stream_mut(&mut self) -> &mut HttpOpusStream {
        &mut self.stream
    }

    pub fn stream_metrics(&self) -> HttpOpusStreamMetrics {
        self.stream.metrics()
    }

    pub fn demux_mut(&mut self) -> &mut WebmOpusDemux {
        &mut self.demux
    }

    pub fn pending_packets_mut(&mut self) -> &mut VecDeque<DemuxedPacket> {
        &mut self.pending_packets
    }

    fn live_position(&self) -> PlaybackPosition {
        let mut position = self.position.lock().unwrap().snapshot();
        position.set_byte_offset(self.stream.position().byte_offset());
        position
    }

    fn sync_position(&mut self) {
        self.position
            .lock()
            .unwrap()
            .set_byte_offset(self.stream.position().byte_offset());
    }
}
