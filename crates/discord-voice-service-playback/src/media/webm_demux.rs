use std::convert::TryFrom;
use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use webm_iterable::WebmIterator;
use webm_iterable::errors::{TagIteratorError, WebmCoercionError};
use webm_iterable::matroska_spec::{Block, Frame, Master, MatroskaSpec, SimpleBlock};

use crate::error::PlaybackError;
use crate::media::opus_queue::{OpusPacketDuration, opus_packet_duration};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DemuxedPacket {
    pub data: Bytes,
    pub timestamp_ms: u64,
    pub duration_ms: u64,
    pub duration_samples: u32,
}

#[derive(Default)]
pub struct WebmOpusDemux {
    pending: BytesMut,
    state: WebmDemuxState,
}

impl WebmOpusDemux {
    pub fn push_bytes(&mut self, chunk: Bytes) {
        self.pending.extend_from_slice(&chunk);
    }

    pub fn drain_packets(&mut self) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }

        match self.state.parse_available(self.pending.as_ref())? {
            ParseOutcome::Complete { state, packets } => {
                self.state = state;
                self.pending.clear();
                Ok(packets)
            }
            ParseOutcome::Incomplete {
                state,
                packets,
                tail_start,
            } if tail_start > 0 && !packets.is_empty() => {
                self.state = state;
                let tail_start = tail_start.min(self.pending.len());
                let _ = self.pending.split_to(tail_start);
                Ok(packets)
            }
            ParseOutcome::Incomplete { .. } | ParseOutcome::Stalled => Ok(Vec::new()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TrackState {
    number: u64,
}

#[derive(Clone, Debug)]
struct WebmDemuxState {
    timestamp_scale_ns: u64,
    cluster_timestamp: u64,
    opus_track: Option<TrackState>,
}

#[derive(Debug)]
enum ParseOutcome {
    Complete {
        state: WebmDemuxState,
        packets: Vec<DemuxedPacket>,
    },
    Incomplete {
        state: WebmDemuxState,
        packets: Vec<DemuxedPacket>,
        tail_start: usize,
    },
    Stalled,
}

impl Default for WebmDemuxState {
    fn default() -> Self {
        Self {
            timestamp_scale_ns: 1_000_000,
            cluster_timestamp: 0,
            opus_track: None,
        }
    }
}

impl WebmDemuxState {
    fn parse_available(&self, input: &[u8]) -> Result<ParseOutcome, PlaybackError> {
        let tags_to_buffer = [MatroskaSpec::TrackEntry(Master::Start)];
        let mut iterator = WebmIterator::new(Cursor::new(input), &tags_to_buffer);
        iterator.emit_master_end_when_eof(false);

        let mut state = self.clone();
        let mut packets = Vec::new();

        for tag in iterator {
            let tag = match tag {
                Ok(tag) => tag,
                Err(TagIteratorError::UnexpectedEOF { tag_start, .. }) => {
                    return Ok(ParseOutcome::Incomplete {
                        state,
                        packets,
                        tail_start: tag_start,
                    });
                }
                Err(TagIteratorError::CorruptedFileData(_))
                | Err(TagIteratorError::CorruptedTagData { .. })
                | Err(TagIteratorError::ReadError { .. }) => return Ok(ParseOutcome::Stalled),
            };
            state.process_tag(tag, &mut packets)?;
        }

        Ok(ParseOutcome::Complete { state, packets })
    }

    fn process_tag(
        &mut self,
        tag: MatroskaSpec,
        packets: &mut Vec<DemuxedPacket>,
    ) -> Result<(), PlaybackError> {
        match tag {
            MatroskaSpec::TimestampScale(scale) => {
                self.timestamp_scale_ns = scale;
            }
            MatroskaSpec::Timestamp(timestamp) => {
                self.cluster_timestamp = timestamp;
            }
            MatroskaSpec::TrackEntry(Master::Full(children)) => {
                self.update_track(children);
            }
            MatroskaSpec::SimpleBlock(data) => {
                let block =
                    SimpleBlock::try_from(data.as_slice()).map_err(map_webm_coercion_error)?;
                packets.extend(self.extract_simple_block(block)?);
            }
            MatroskaSpec::Block(data) => {
                let block = Block::try_from(data.as_slice()).map_err(map_webm_coercion_error)?;
                packets.extend(self.extract_block(block)?);
            }
            MatroskaSpec::BlockGroup(Master::Full(children)) => {
                packets.extend(self.extract_block_group(children)?);
            }
            _ => {}
        }

        Ok(())
    }

    fn update_track(&mut self, children: Vec<MatroskaSpec>) {
        let mut number = None;
        let mut codec_id = None;
        let mut track_type = None;

        for child in children {
            match child {
                MatroskaSpec::TrackNumber(value) => number = Some(value),
                MatroskaSpec::CodecID(value) => codec_id = Some(value),
                MatroskaSpec::TrackType(value) => track_type = Some(value),
                _ => {}
            }
        }

        if track_type == Some(2) && codec_id.as_deref() == Some("A_OPUS") {
            self.opus_track = number.map(|number| TrackState { number });
        }
    }

    fn extract_simple_block(
        &self,
        block: SimpleBlock<'_>,
    ) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        if !self.is_target_track(block.track) {
            return Ok(Vec::new());
        }

        let frames = block.read_frame_data().map_err(map_webm_coercion_error)?;
        self.frames_to_packets(block.timestamp, frames)
    }

    fn extract_block_group(
        &self,
        children: Vec<MatroskaSpec>,
    ) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        let mut block_data = None;

        for child in children {
            match child {
                MatroskaSpec::Block(data) => {
                    block_data = Some(data);
                }
                _ => {}
            }
        }

        let Some(block_data) = block_data else {
            return Ok(Vec::new());
        };
        let block = Block::try_from(block_data.as_slice()).map_err(map_webm_coercion_error)?;
        self.extract_block(block)
    }

    fn extract_block(&self, block: Block<'_>) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        if !self.is_target_track(block.track) {
            return Ok(Vec::new());
        }

        let frames = block.read_frame_data().map_err(map_webm_coercion_error)?;
        self.frames_to_packets(block.timestamp, frames)
    }

    fn frames_to_packets(
        &self,
        relative_timestamp: i16,
        frames: Vec<Frame<'_>>,
    ) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }

        let durations = opus_frame_durations(&frames).ok_or(PlaybackError::MediaParse(
            "unsupported opus packet duration",
        ))?;

        let mut timestamp_ms = self.block_timestamp_ms(relative_timestamp)?;
        let mut packets = Vec::with_capacity(frames.len());

        for (frame, duration) in frames.into_iter().zip(durations) {
            packets.push(DemuxedPacket {
                data: Bytes::copy_from_slice(frame.data),
                timestamp_ms,
                duration_ms: duration.ms,
                duration_samples: duration.samples,
            });
            timestamp_ms = timestamp_ms.saturating_add(duration.ms);
        }

        Ok(packets)
    }

    fn block_timestamp_ms(&self, relative_timestamp: i16) -> Result<u64, PlaybackError> {
        let total_ticks = i128::from(self.cluster_timestamp) + i128::from(relative_timestamp);
        if total_ticks < 0 {
            return Err(PlaybackError::MediaParse("negative block timestamp"));
        }

        self.scale_timestamp_to_ms(total_ticks as u64)
    }

    fn scale_timestamp_to_ms(&self, ticks: u64) -> Result<u64, PlaybackError> {
        let scaled = u128::from(ticks)
            .checked_mul(u128::from(self.timestamp_scale_ns))
            .ok_or(PlaybackError::MediaParse("timestamp overflow"))?;
        Ok((scaled / 1_000_000) as u64)
    }

    fn is_target_track(&self, track_number: u64) -> bool {
        self.opus_track
            .map(|track| track.number == track_number)
            .unwrap_or(true)
    }
}

fn opus_frame_durations(frames: &[Frame<'_>]) -> Option<Vec<OpusPacketDuration>> {
    frames
        .iter()
        .map(|frame| opus_packet_duration(frame.data))
        .collect()
}

fn map_webm_coercion_error(error: WebmCoercionError) -> PlaybackError {
    PlaybackError::MediaParseDetail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use discord_voice_service_test_support::fixtures::load_fixture_bytes;

    #[test]
    fn demux_does_not_retain_completed_audio_prefix() {
        let fixture = load_fixture_bytes("audio-long.webm");
        let mut one_shot_demux = WebmOpusDemux::default();
        one_shot_demux.push_bytes(Bytes::copy_from_slice(&fixture));
        let expected_packet_count = one_shot_demux.drain_packets().unwrap().len();

        let mut demux = WebmOpusDemux::default();
        let mut packet_count = 0usize;
        let mut max_pending_after_audio = 0usize;

        for chunk in fixture.chunks(256) {
            demux.push_bytes(Bytes::copy_from_slice(chunk));
            packet_count += demux.drain_packets().unwrap().len();

            if packet_count > 0 {
                max_pending_after_audio = max_pending_after_audio.max(demux.pending.len());
            }
        }

        assert!(packet_count > 1_000, "fixture produced too few packets");
        assert_eq!(packet_count, expected_packet_count);
        assert!(
            max_pending_after_audio < 16 * 1024,
            "demux retained {max_pending_after_audio} bytes after audio packets started"
        );
    }
}
