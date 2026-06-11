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
        if self.pending_pcm.is_empty() && is_discord_passthrough_packet(&packet) {
            return Ok(vec![
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
            ]);
        }

        self.decode_packet(&packet)?;
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

    fn decode_packet(&mut self, packet: &DemuxedPacket) -> Result<(), PlaybackError> {
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

        match channels {
            1 => {
                for sample in pcm {
                    self.pending_pcm.push(sample);
                    self.pending_pcm.push(sample);
                }
            }
            2 => self.pending_pcm.extend_from_slice(&pcm),
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

fn opus_packet_channels(packet: &[u8]) -> Option<usize> {
    let toc = *packet.first()?;
    Some(if toc & 0x04 != 0 { 2 } else { 1 })
}

fn is_discord_passthrough_packet(packet: &DemuxedPacket) -> bool {
    packet.duration_samples == DISCORD_FRAME_SAMPLES_U32
        && opus_packet_channels(packet.data.as_ref()) == Some(DISCORD_CHANNELS)
}
