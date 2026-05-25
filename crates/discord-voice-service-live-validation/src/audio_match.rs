use std::collections::HashMap;

use anyhow::{Context, Result};
use bytes::Bytes;
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

#[derive(Debug, Clone)]
pub struct StreamingAudioMatcher {
    expected_frames: usize,
    expected_indices_by_payload: HashMap<Bytes, Vec<usize>>,
    next_expected_index: usize,
    received_frames: usize,
    matched_frames: usize,
    consecutive_silence_frames: usize,
    sustained_silence: bool,
}

impl StreamingAudioMatcher {
    pub fn new(expected: &[OpusFrame]) -> Self {
        let mut expected_indices_by_payload = HashMap::<Bytes, Vec<usize>>::new();
        for (index, frame) in expected.iter().enumerate() {
            expected_indices_by_payload
                .entry(frame.data.clone())
                .or_default()
                .push(index);
        }

        Self {
            expected_frames: expected.len(),
            expected_indices_by_payload,
            next_expected_index: 0,
            received_frames: 0,
            matched_frames: 0,
            consecutive_silence_frames: 0,
            sustained_silence: false,
        }
    }

    pub fn observe(&mut self, frame: &ObservedAudioFrame) -> ObserverAudioEvidence {
        self.received_frames += 1;
        self.record_silence(frame);
        self.record_ordered_match(frame);
        self.evidence()
    }

    pub fn observe_from_speaker(
        &mut self,
        frame: &ObservedAudioFrame,
        expected_speaker_user_id: &str,
    ) -> ObserverAudioEvidence {
        if frame.user_id == expected_speaker_user_id {
            self.observe(frame)
        } else {
            self.evidence()
        }
    }

    pub fn evidence(&self) -> ObserverAudioEvidence {
        let match_ratio = if self.expected_frames == 0 {
            0.0
        } else {
            self.matched_frames as f32 / self.expected_frames as f32
        };
        let verified = self.matched_frames >= MIN_MATCHED_FRAMES
            && match_ratio >= MIN_MATCH_RATIO
            && !self.sustained_silence;

        ObserverAudioEvidence {
            verified,
            received_frames: self.received_frames,
            matched_frames: self.matched_frames,
            match_ratio,
        }
    }

    fn record_silence(&mut self, frame: &ObservedAudioFrame) {
        if frame.payload.as_ref() == OPUS_SILENCE_FRAME {
            self.consecutive_silence_frames += 1;
            if self.consecutive_silence_frames >= SUSTAINED_SILENCE_FRAMES {
                self.sustained_silence = true;
            }
        } else {
            self.consecutive_silence_frames = 0;
        }
    }

    fn record_ordered_match(&mut self, frame: &ObservedAudioFrame) {
        let Some(indices) = self.expected_indices_by_payload.get(&frame.payload) else {
            return;
        };
        let candidate_position = match indices.binary_search(&self.next_expected_index) {
            Ok(position) => position,
            Err(position) => position,
        };
        let Some(matched_index) = indices.get(candidate_position) else {
            return;
        };

        self.matched_frames += 1;
        self.next_expected_index = matched_index + 1;
    }
}

pub fn compare_expected_and_observed(
    expected: &[OpusFrame],
    observed: &[ObservedAudioFrame],
) -> ObserverAudioEvidence {
    let mut matcher = StreamingAudioMatcher::new(expected);
    for frame in observed {
        matcher.observe(frame);
    }
    matcher.evidence()
}

pub fn compare_expected_and_observed_from_speaker(
    expected: &[OpusFrame],
    observed: &[ObservedAudioFrame],
    expected_speaker_user_id: &str,
) -> ObserverAudioEvidence {
    let mut matcher = StreamingAudioMatcher::new(expected);
    for frame in observed {
        matcher.observe_from_speaker(frame, expected_speaker_user_id);
    }
    matcher.evidence()
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
