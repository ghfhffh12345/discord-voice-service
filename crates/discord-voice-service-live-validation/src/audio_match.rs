use anyhow::{Context, Result};
use discord_voice_service_playback::media::opus_queue::{OpusFrame, OpusFrameQueue};
use discord_voice_service_playback::{PlaybackWorker, YtMusicClient};
use discord_voice_service_voice::ObservedAudioFrame;
use serde::Serialize;

const MIN_MATCH_RATIO: f32 = 0.80;
const MIN_MATCHED_FRAMES: usize = 4;
const SUSTAINED_SILENCE_FRAMES: usize = 10;
const OPUS_SILENCE_FRAME: &[u8] = &[0xf8, 0xff, 0xfe];

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ObserverAudioEvidence {
    pub verified: bool,
    pub received_frames: usize,
    pub matched_frames: usize,
    pub match_ratio: f32,
}

pub fn compare_expected_and_observed(
    expected: &[OpusFrame],
    observed: &[ObservedAudioFrame],
) -> ObserverAudioEvidence {
    let matched_frames = longest_ordered_payload_match(expected, observed);
    let match_ratio = if expected.is_empty() {
        0.0
    } else {
        matched_frames as f32 / expected.len() as f32
    };
    let verified = matched_frames >= MIN_MATCHED_FRAMES
        && match_ratio >= MIN_MATCH_RATIO
        && !has_sustained_silence(observed);

    ObserverAudioEvidence {
        verified,
        received_frames: observed.len(),
        matched_frames,
        match_ratio,
    }
}

pub async fn build_expected_track_frames(
    ytmusic_endpoint: &str,
    video_id: &str,
) -> Result<Vec<OpusFrame>> {
    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(ytmusic_endpoint.to_owned())
            .await
            .context("connect ytmusic client for expected audio")?,
    );
    let mut queue = OpusFrameQueue::new(32);
    let mut source = worker
        .prepare(video_id, &mut queue)
        .await
        .context("prepare expected validation track")?;
    let mut frames = Vec::new();

    loop {
        while let Some(frame) = queue.pop() {
            frames.push(frame);
        }

        worker
            .fill_queue(&mut source, &mut queue)
            .await
            .context("read expected validation track frames")?;
        if queue.is_empty() {
            break;
        }
    }

    Ok(frames)
}

fn longest_ordered_payload_match(expected: &[OpusFrame], observed: &[ObservedAudioFrame]) -> usize {
    if expected.is_empty() || observed.is_empty() {
        return 0;
    }

    let mut previous = vec![0usize; observed.len() + 1];
    let mut current = vec![0usize; observed.len() + 1];

    for expected_frame in expected {
        for (observed_index, observed_frame) in observed.iter().enumerate() {
            current[observed_index + 1] = if expected_frame.data == observed_frame.payload {
                previous[observed_index] + 1
            } else {
                previous[observed_index + 1].max(current[observed_index])
            };
        }
        std::mem::swap(&mut previous, &mut current);
        current.fill(0);
    }

    previous[observed.len()]
}

fn has_sustained_silence(observed: &[ObservedAudioFrame]) -> bool {
    let mut consecutive_silence = 0usize;

    for frame in observed {
        if frame.payload.as_ref() == OPUS_SILENCE_FRAME {
            consecutive_silence += 1;
            if consecutive_silence >= SUSTAINED_SILENCE_FRAMES {
                return true;
            }
        } else {
            consecutive_silence = 0;
        }
    }

    false
}
