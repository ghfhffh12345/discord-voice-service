use anyhow::{Context, Result, bail};
use opus_rs::OpusDecoder;
use serde::Serialize;
use std::panic::{AssertUnwindSafe, catch_unwind};

const SAMPLE_RATE_HZ: i32 = 48_000;
const NON_SILENCE_PEAK_THRESHOLD: f32 = 0.001;

#[derive(Debug, Clone, Copy)]
pub struct ObservedOpusPacket<'a> {
    pub sequence: u16,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct AudioValidationStats {
    pub observed_packet_count: u64,
    pub decoded_audio_ms: u64,
    pub non_silent_audio_ms: u64,
    pub max_peak_amplitude: f32,
    pub rms_amplitude: f32,
    pub first_sequence: Option<u16>,
    pub last_sequence: Option<u16>,
}

pub fn analyze_opus_packets<'a, I>(packets: I) -> Result<AudioValidationStats>
where
    I: IntoIterator<Item = ObservedOpusPacket<'a>>,
{
    let mut mono_decoder = None;
    let mut stereo_decoder = None;
    let mut stats = AudioValidationStats::default();
    let mut total_squared_amplitude = 0.0f64;
    let mut total_samples = 0usize;

    for packet in packets {
        if packet.payload.is_empty() {
            bail!("observed opus payload must not be empty");
        }

        let frame_samples = opus_packet_frame_samples(packet.payload).with_context(|| {
            format!(
                "unsupported opus packet header at sequence {}",
                packet.sequence
            )
        })?;
        let channels = opus_packet_channels(packet.payload).with_context(|| {
            format!(
                "unsupported opus packet channel layout at sequence {}",
                packet.sequence
            )
        })?;
        let duration_ms = (frame_samples as u64) / 48;
        let decoder = match channels {
            1 => mono_decoder.get_or_insert(
                OpusDecoder::new(SAMPLE_RATE_HZ, 1)
                    .map_err(anyhow::Error::msg)
                    .context("create mono opus decoder")?,
            ),
            2 => stereo_decoder.get_or_insert(
                OpusDecoder::new(SAMPLE_RATE_HZ, 2)
                    .map_err(anyhow::Error::msg)
                    .context("create stereo opus decoder")?,
            ),
            _ => bail!("unsupported opus channel count: {channels}"),
        };
        let mut pcm = vec![0.0f32; frame_samples * channels];
        let decode_result = catch_unwind(AssertUnwindSafe(|| {
            decoder.decode(packet.payload, frame_samples, &mut pcm)
        }))
        .map_err(|_| {
            anyhow::anyhow!(
                "decode opus packet panicked at sequence {}",
                packet.sequence
            )
        })?;
        decode_result
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("decode opus packet at sequence {}", packet.sequence))?;

        let packet_peak = pcm
            .iter()
            .fold(0.0f32, |peak, sample| peak.max(sample.abs()));
        let packet_square_sum: f64 = pcm
            .iter()
            .map(|sample| {
                let value = f64::from(*sample);
                value * value
            })
            .sum();

        stats.observed_packet_count += 1;
        stats.decoded_audio_ms += duration_ms;
        if packet_peak > NON_SILENCE_PEAK_THRESHOLD {
            stats.non_silent_audio_ms += duration_ms;
        }
        stats.max_peak_amplitude = stats.max_peak_amplitude.max(packet_peak);
        stats.first_sequence.get_or_insert(packet.sequence);
        stats.last_sequence = Some(packet.sequence);

        total_squared_amplitude += packet_square_sum;
        total_samples += pcm.len();
    }

    if stats.observed_packet_count == 0 {
        bail!("expected at least one observed opus packet");
    }

    stats.rms_amplitude = if total_samples == 0 {
        0.0
    } else {
        ((total_squared_amplitude / total_samples as f64).sqrt()) as f32
    };

    Ok(stats)
}

fn opus_packet_frame_samples(packet: &[u8]) -> Option<usize> {
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

    Some(samples_per_frame * frames)
}

fn opus_packet_channels(packet: &[u8]) -> Option<usize> {
    let toc = *packet.first()?;
    Some(if toc & 0x04 != 0 { 2 } else { 1 })
}
