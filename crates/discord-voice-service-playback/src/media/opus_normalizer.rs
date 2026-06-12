use bytes::Bytes;
use opus_rs::{Application, OpusDecoder, OpusEncoder};

use crate::error::PlaybackError;

use super::opus_queue::{OpusFrame, duration_ms_from_samples};
use super::webm_demux::DemuxedPacket;

pub const DISCORD_FRAME_SAMPLES: usize = 960;
const DISCORD_FRAME_SAMPLES_U32: u32 = 960;
const DISCORD_FRAME_MS: u64 = 20;
const DISCORD_CHANNELS: usize = 2;
const OPUS_SAMPLE_RATE_HZ: i32 = 48_000;
const MAX_ENCODED_PACKET_BYTES: usize = 1_500;
const TIMELINE_DISCONTINUITY_TOLERANCE_SAMPLES: u64 = 120;

pub struct DiscordOpusFrameNormalizer {
    mono_decoder: Option<OpusDecoder>,
    stereo_decoder: Option<OpusDecoder>,
    encoder: Option<OpusEncoder>,
    pending_pcm: Vec<f32>,
    next_output_position_samples: Option<u64>,
    encode_buffer: Vec<u8>,
}

impl DiscordOpusFrameNormalizer {
    pub fn new() -> Self {
        Self {
            mono_decoder: None,
            stereo_decoder: None,
            encoder: None,
            pending_pcm: Vec::new(),
            next_output_position_samples: None,
            encode_buffer: vec![0; MAX_ENCODED_PACKET_BYTES],
        }
    }

    pub fn push_packet(&mut self, packet: DemuxedPacket) -> Result<Vec<OpusFrame>, PlaybackError> {
        let alignment = self.align_to_packet_timeline(&packet)?;
        if alignment.drop_packet {
            return self.emit_ready_frames();
        }

        if self.pending_pcm.is_empty()
            && alignment.trim_start_samples == 0
            && alignment.gap_samples % u64::from(DISCORD_FRAME_SAMPLES_U32) == 0
            && is_discord_passthrough_packet(&packet)
        {
            let mut frames = Vec::new();
            if alignment.gap_samples > 0 {
                self.append_silence_samples(alignment.gap_samples)?;
                frames.extend(self.emit_ready_frames()?);
            }
            self.next_output_position_samples = Some(
                packet
                    .timestamp_samples
                    .saturating_add(u64::from(packet.duration_samples)),
            );
            frames.push(
                OpusFrame::with_duration_samples(
                    packet.data,
                    DISCORD_FRAME_MS,
                    DISCORD_FRAME_SAMPLES_U32,
                )
                .with_exact_metadata(
                    packet.timestamp_ms,
                    packet.timestamp_samples,
                    None,
                    0,
                ),
            );
            return Ok(frames);
        }

        if alignment.gap_samples > 0 {
            self.append_silence_samples(alignment.gap_samples)?;
        }

        self.decode_packet(&packet, alignment.trim_start_samples)?;
        self.emit_ready_frames()
    }

    pub fn flush(&mut self) -> Result<Vec<OpusFrame>, PlaybackError> {
        if self.pending_pcm.is_empty() {
            return Ok(Vec::new());
        }

        let frame_values = DISCORD_FRAME_SAMPLES.saturating_mul(DISCORD_CHANNELS);
        self.pending_pcm.resize(frame_values, 0.0);
        self.emit_ready_frames()
    }

    fn align_to_packet_timeline(
        &mut self,
        packet: &DemuxedPacket,
    ) -> Result<PacketTimelineAlignment, PlaybackError> {
        let Some(next_output_position_samples) = self.next_output_position_samples else {
            self.next_output_position_samples = Some(packet.timestamp_samples);
            return Ok(PacketTimelineAlignment {
                gap_samples: 0,
                trim_start_samples: 0,
                drop_packet: false,
            });
        };

        let buffered_until_samples =
            next_output_position_samples.saturating_add(self.pending_pcm_samples()?);
        if packet.timestamp_samples > buffered_until_samples {
            let gap_samples = packet
                .timestamp_samples
                .saturating_sub(buffered_until_samples);
            if gap_samples < TIMELINE_DISCONTINUITY_TOLERANCE_SAMPLES {
                if self.pending_pcm.is_empty() {
                    self.next_output_position_samples = Some(packet.timestamp_samples);
                }
                return Ok(PacketTimelineAlignment {
                    gap_samples: 0,
                    trim_start_samples: 0,
                    drop_packet: false,
                });
            }
            return Ok(PacketTimelineAlignment {
                gap_samples,
                trim_start_samples: 0,
                drop_packet: false,
            });
        }

        let overlap_samples = buffered_until_samples.saturating_sub(packet.timestamp_samples);
        if overlap_samples < TIMELINE_DISCONTINUITY_TOLERANCE_SAMPLES {
            if self.pending_pcm.is_empty() {
                self.next_output_position_samples = Some(packet.timestamp_samples);
            }
            return Ok(PacketTimelineAlignment {
                gap_samples: 0,
                trim_start_samples: 0,
                drop_packet: false,
            });
        }

        if overlap_samples >= u64::from(packet.duration_samples) {
            return Ok(PacketTimelineAlignment {
                gap_samples: 0,
                trim_start_samples: u64::from(packet.duration_samples),
                drop_packet: true,
            });
        }

        Ok(PacketTimelineAlignment {
            gap_samples: 0,
            trim_start_samples: overlap_samples,
            drop_packet: false,
        })
    }

    fn pending_pcm_samples(&self) -> Result<u64, PlaybackError> {
        let samples = self.pending_pcm.len() / DISCORD_CHANNELS;
        samples
            .try_into()
            .map_err(|_| PlaybackError::MediaParse("pending opus timeline overflow"))
    }

    fn append_silence_samples(&mut self, samples: u64) -> Result<(), PlaybackError> {
        let sample_values = samples
            .checked_mul(DISCORD_CHANNELS as u64)
            .and_then(|values| usize::try_from(values).ok())
            .ok_or(PlaybackError::MediaParse("opus timeline gap is too large"))?;
        let new_len = self
            .pending_pcm
            .len()
            .checked_add(sample_values)
            .ok_or(PlaybackError::MediaParse("opus timeline gap is too large"))?;
        self.pending_pcm.resize(new_len, 0.0);
        Ok(())
    }

    fn decode_packet(
        &mut self,
        packet: &DemuxedPacket,
        trim_start_samples: u64,
    ) -> Result<(), PlaybackError> {
        let channels = opus_packet_channels(packet.data.as_ref())
            .ok_or(PlaybackError::MediaParse("unsupported opus channel layout"))?;
        let frame_samples = usize::try_from(packet.duration_samples).map_err(|_| {
            PlaybackError::MediaParse("opus packet duration exceeds platform usize")
        })?;
        if frame_samples == 0 {
            return Err(PlaybackError::MediaParse("opus packet duration is zero"));
        }

        if self.next_output_position_samples.is_none() {
            self.next_output_position_samples = Some(packet.timestamp_samples);
        }

        let mut pcm = vec![0.0f32; frame_samples.saturating_mul(channels)];
        let decoded_samples = self
            .decoder_for_channels(channels)?
            .decode(packet.data.as_ref(), frame_samples, &mut pcm)
            .map_err(|error| {
                PlaybackError::MediaParseDetail(format!("decode opus packet: {error}"))
            })?;
        if decoded_samples != frame_samples {
            return Err(PlaybackError::MediaParseDetail(format!(
                "decoded opus packet returned {decoded_samples} samples; expected {frame_samples}"
            )));
        }

        let trim_start_samples = usize::try_from(trim_start_samples)
            .unwrap_or(usize::MAX)
            .min(decoded_samples);
        if trim_start_samples == decoded_samples {
            return Ok(());
        }

        match channels {
            1 => {
                for sample in pcm.into_iter().skip(trim_start_samples) {
                    self.pending_pcm.push(sample);
                    self.pending_pcm.push(sample);
                }
            }
            2 => {
                let trim_values = trim_start_samples.saturating_mul(channels);
                self.pending_pcm.extend_from_slice(&pcm[trim_values..]);
            }
            _ => {
                return Err(PlaybackError::MediaParse("unsupported opus channel layout"));
            }
        }
        Ok(())
    }

    fn decoder_for_channels(&mut self, channels: usize) -> Result<&mut OpusDecoder, PlaybackError> {
        let decoder = match channels {
            1 => &mut self.mono_decoder,
            2 => &mut self.stereo_decoder,
            _ => {
                return Err(PlaybackError::MediaParse("unsupported opus channel layout"));
            }
        };
        if decoder.is_none() {
            *decoder = Some(
                OpusDecoder::new(OPUS_SAMPLE_RATE_HZ, channels).map_err(|error| {
                    PlaybackError::MediaParseDetail(format!("create opus decoder: {error}"))
                })?,
            );
        }
        decoder.as_mut().ok_or(PlaybackError::InvalidState(
            "opus normalizer decoder missing",
        ))
    }

    fn ensure_encoder(&mut self) -> Result<(), PlaybackError> {
        if self.encoder.is_some() {
            return Ok(());
        }
        let mut encoder =
            OpusEncoder::new(OPUS_SAMPLE_RATE_HZ, DISCORD_CHANNELS, Application::Audio).map_err(
                |error| PlaybackError::MediaParseDetail(format!("create opus encoder: {error}")),
            )?;
        encoder.bitrate_bps = 128_000;
        self.encoder = Some(encoder);
        Ok(())
    }

    fn emit_ready_frames(&mut self) -> Result<Vec<OpusFrame>, PlaybackError> {
        self.ensure_encoder()?;
        let frame_values = DISCORD_FRAME_SAMPLES.saturating_mul(DISCORD_CHANNELS);
        let mut frames = Vec::new();

        while self.pending_pcm.len() >= frame_values {
            let packet = {
                let pcm = &self.pending_pcm[..frame_values];
                let encoded_len = self
                    .encoder
                    .as_mut()
                    .ok_or(PlaybackError::InvalidState(
                        "opus normalizer encoder missing",
                    ))?
                    .encode(pcm, DISCORD_FRAME_SAMPLES, &mut self.encode_buffer)
                    .map_err(|error| {
                        PlaybackError::MediaParseDetail(format!(
                            "encode discord opus frame: {error}"
                        ))
                    })?;
                Bytes::copy_from_slice(&self.encode_buffer[..encoded_len])
            };

            let source_position_samples = self.next_output_position_samples.unwrap_or(0);
            frames.push(
                OpusFrame::with_duration_samples(
                    packet,
                    DISCORD_FRAME_MS,
                    DISCORD_FRAME_SAMPLES_U32,
                )
                .with_exact_metadata(
                    duration_ms_from_samples(source_position_samples),
                    source_position_samples,
                    None,
                    0,
                ),
            );
            self.next_output_position_samples =
                Some(source_position_samples.saturating_add(u64::from(DISCORD_FRAME_SAMPLES_U32)));
            self.pending_pcm.drain(..frame_values);
        }

        Ok(frames)
    }
}

impl Default for DiscordOpusFrameNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
struct PacketTimelineAlignment {
    gap_samples: u64,
    trim_start_samples: u64,
    drop_packet: bool,
}

fn opus_packet_channels(packet: &[u8]) -> Option<usize> {
    let toc = *packet.first()?;
    Some(if toc & 0x04 != 0 { 2 } else { 1 })
}

fn is_discord_passthrough_packet(packet: &DemuxedPacket) -> bool {
    packet.duration_samples == DISCORD_FRAME_SAMPLES_U32
        && opus_packet_channels(packet.data.as_ref()) == Some(DISCORD_CHANNELS)
}
