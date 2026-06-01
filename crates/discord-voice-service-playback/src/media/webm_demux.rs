use std::convert::TryFrom;
use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use webm_iterable::WebmIterator;
use webm_iterable::errors::{TagIteratorError, WebmCoercionError};
use webm_iterable::matroska_spec::{Block, Frame, Master, MatroskaSpec, SimpleBlock};

use crate::error::PlaybackError;

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
    emitted_packets: usize,
}

impl WebmOpusDemux {
    pub fn push_bytes(&mut self, chunk: Bytes) {
        self.pending.extend_from_slice(&chunk);
    }

    pub fn drain_packets(&mut self) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }

        match WebmDemuxState::parse(self.pending.as_ref())? {
            Some(packets) => {
                if self.emitted_packets > packets.len() {
                    return Err(PlaybackError::MediaParse(
                        "demux packet cursor exceeded parsed packet count",
                    ));
                }

                let new_packets = packets[self.emitted_packets..].to_vec();
                self.emitted_packets = packets.len();
                Ok(new_packets)
            }
            None => Ok(Vec::new()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TrackState {
    number: u64,
    default_duration_ms: Option<u64>,
}

#[derive(Debug)]
struct WebmDemuxState {
    timestamp_scale_ns: u64,
    cluster_timestamp: u64,
    opus_track: Option<TrackState>,
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
    fn parse(input: &[u8]) -> Result<Option<Vec<DemuxedPacket>>, PlaybackError> {
        let tags_to_buffer = [
            MatroskaSpec::TrackEntry(Master::Start),
            MatroskaSpec::BlockGroup(Master::Start),
        ];
        let iterator = WebmIterator::new(Cursor::new(input), &tags_to_buffer);
        let mut state = Self::default();
        let mut packets = Vec::new();

        for tag in iterator {
            let tag = match tag {
                Ok(tag) => tag,
                Err(TagIteratorError::UnexpectedEOF { .. })
                | Err(TagIteratorError::CorruptedFileData(_))
                | Err(TagIteratorError::CorruptedTagData { .. })
                | Err(TagIteratorError::ReadError { .. }) => return Ok(None),
            };
            if state.process_tag(tag, &mut packets).is_err() {
                return Ok(None);
            }
        }

        Ok(Some(packets))
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
        let mut default_duration_ns = None;

        for child in children {
            match child {
                MatroskaSpec::TrackNumber(value) => number = Some(value),
                MatroskaSpec::CodecID(value) => codec_id = Some(value),
                MatroskaSpec::TrackType(value) => track_type = Some(value),
                MatroskaSpec::DefaultDuration(value) => default_duration_ns = Some(value),
                _ => {}
            }
        }

        if track_type == Some(2) && codec_id.as_deref() == Some("A_OPUS") {
            self.opus_track = number.map(|number| TrackState {
                number,
                default_duration_ms: default_duration_ns.map(|value| value / 1_000_000),
            });
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
        let fallback_duration = self.opus_track.and_then(|track| track.default_duration_ms);
        self.frames_to_packets(block.timestamp, frames, None, fallback_duration)
    }

    fn extract_block_group(
        &self,
        children: Vec<MatroskaSpec>,
    ) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        let mut block_data = None;
        let mut block_duration_ms = None;

        for child in children {
            match child {
                MatroskaSpec::Block(data) => {
                    block_data = Some(data);
                }
                MatroskaSpec::BlockDuration(duration) => {
                    block_duration_ms = Some(self.scale_timestamp_to_ms(duration)?);
                }
                _ => {}
            }
        }

        let Some(block_data) = block_data else {
            return Ok(Vec::new());
        };
        let block = Block::try_from(block_data.as_slice()).map_err(map_webm_coercion_error)?;

        if !self.is_target_track(block.track) {
            return Ok(Vec::new());
        }

        let frames = block.read_frame_data().map_err(map_webm_coercion_error)?;
        let fallback_duration = self.opus_track.and_then(|track| track.default_duration_ms);
        self.frames_to_packets(
            block.timestamp,
            frames,
            block_duration_ms,
            fallback_duration,
        )
    }

    fn frames_to_packets(
        &self,
        relative_timestamp: i16,
        frames: Vec<Frame<'_>>,
        block_duration_ms: Option<u64>,
        fallback_duration_ms: Option<u64>,
    ) -> Result<Vec<DemuxedPacket>, PlaybackError> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }

        let durations = if let Some(durations) = opus_frame_durations(&frames) {
            durations
        } else if let Some(block_duration_ms) = block_duration_ms {
            distribute_duration(block_duration_ms, frames.len())
        } else if let Some(fallback_duration_ms) = fallback_duration_ms {
            distribute_duration(fallback_duration_ms, frames.len())
        } else {
            distribute_duration(20, frames.len())
        };

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PacketDuration {
    ms: u64,
    samples: u32,
}

fn opus_frame_durations(frames: &[Frame<'_>]) -> Option<Vec<PacketDuration>> {
    frames
        .iter()
        .map(|frame| opus_packet_duration(frame.data))
        .collect()
}

fn opus_packet_duration(packet: &[u8]) -> Option<PacketDuration> {
    let toc = *packet.first()?;
    let samples_per_frame = if (toc & 0x80) != 0 {
        let shift = usize::from((toc >> 3) & 0x03);
        (48_000usize << shift) / 400
    } else if (toc & 0x60) == 0x60 {
        if (toc & 0x08) != 0 {
            48_000usize / 50
        } else {
            48_000usize / 100
        }
    } else {
        let index = usize::from((toc >> 3) & 0x03);
        if index == 3 {
            48_000usize * 60 / 1_000
        } else {
            (48_000usize << index) / 100
        }
    };

    let frames = match toc & 0x03 {
        0 => 1usize,
        1 | 2 => 2usize,
        3 => usize::from(*packet.get(1)? & 0x3F),
        _ => return None,
    };

    let samples = samples_per_frame.checked_mul(frames)?.try_into().ok()?;
    Some(PacketDuration {
        ms: u64::from(samples) / 48,
        samples,
    })
}

fn distribute_duration(total_duration_ms: u64, parts: usize) -> Vec<PacketDuration> {
    if parts == 0 {
        return Vec::new();
    }

    let base = total_duration_ms / parts as u64;
    let remainder = total_duration_ms % parts as u64;
    let mut durations = Vec::with_capacity(parts);

    for index in 0..parts {
        let extra = u64::from((index as u64) < remainder);
        let ms = base + extra;
        durations.push(PacketDuration {
            ms,
            samples: crate::media::opus_queue::samples_from_duration_ms(ms),
        });
    }

    durations
}

fn map_webm_coercion_error(error: WebmCoercionError) -> PlaybackError {
    PlaybackError::MediaParseDetail(error.to_string())
}
