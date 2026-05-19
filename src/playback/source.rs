use std::collections::VecDeque;

use crate::media::http_stream::HttpOpusStream;
use crate::media::position::PlaybackPosition;
use crate::media::webm_demux::{DemuxedPacket, WebmOpusDemux};
use crate::ytmusic::client::ResolvedPlaybackSource;

pub struct PlaybackSource {
    resolved: ResolvedPlaybackSource,
    stream: HttpOpusStream,
    demux: WebmOpusDemux,
    pending_packets: VecDeque<DemuxedPacket>,
    position: PlaybackPosition,
}

impl PlaybackSource {
    pub fn new(
        resolved: ResolvedPlaybackSource,
        stream: HttpOpusStream,
        demux: WebmOpusDemux,
        pending_packets: VecDeque<DemuxedPacket>,
        position: PlaybackPosition,
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

    pub fn playable_url(&self) -> &str {
        &self.resolved.playable_url
    }

    pub fn position(&self) -> PlaybackPosition {
        self.position
    }

    pub fn position_mut(&mut self) -> &mut PlaybackPosition {
        &mut self.position
    }

    pub fn record_sent_packet(&mut self, duration_ms: u64) {
        self.position.record_sent_packet(duration_ms);
    }

    pub fn stream_mut(&mut self) -> &mut HttpOpusStream {
        &mut self.stream
    }

    pub fn demux_mut(&mut self) -> &mut WebmOpusDemux {
        &mut self.demux
    }

    pub fn pending_packets_mut(&mut self) -> &mut VecDeque<DemuxedPacket> {
        &mut self.pending_packets
    }
}
