use super::ytmusic_client::YtMusicClient;
use crate::error::PlaybackError;
use crate::media::opus_normalizer::DiscordOpusFrameNormalizer;
use crate::media::opus_queue::{OpusFrame, OpusFrameQueue, samples_from_duration_ms_u64};
use crate::media::position::{PlaybackPosition, SharedPlaybackPosition, shared_playback_position};
use crate::media::webm_demux::DemuxedPacket;
use crate::recovery::{PlaybackRecovery, PlaybackRecoveryMetrics, PlaybackResumePosition};
use crate::source::PlaybackSource;

const DEFAULT_PREBUFFER_TARGET_MS: u64 = 80;

pub struct PlaybackWorker {
    current_video_id: Option<String>,
    recovery: PlaybackRecovery,
    position: SharedPlaybackPosition,
    prebuffer_target_ms: u64,
    normalizer: DiscordOpusFrameNormalizer,
    pending_frames: std::collections::VecDeque<OpusFrame>,
}

impl PlaybackWorker {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            current_video_id: None,
            recovery: PlaybackRecovery::new(client),
            position: shared_playback_position(PlaybackPosition::default()),
            prebuffer_target_ms: DEFAULT_PREBUFFER_TARGET_MS,
            normalizer: DiscordOpusFrameNormalizer::new(),
            pending_frames: std::collections::VecDeque::new(),
        }
    }

    pub async fn prepare(
        &mut self,
        video_id: &str,
        queue: &mut OpusFrameQueue,
    ) -> Result<PlaybackSource, PlaybackError> {
        if queue.is_full() {
            return Err(PlaybackError::BufferFull);
        }

        let resume_position = if self.current_video_id.as_deref() == Some(video_id) {
            PlaybackResumePosition::from_samples(
                self.position.lock().unwrap().sent_duration_samples(),
            )
        } else {
            self.position = shared_playback_position(PlaybackPosition::default());
            PlaybackResumePosition::default()
        };
        self.normalizer = DiscordOpusFrameNormalizer::new();
        self.pending_frames.clear();

        let mut source = self.recovery.recover(video_id, resume_position).await?;
        self.position = source.shared_position();

        self.fill_queue_to_duration_ms_for_video(
            Some(video_id),
            &mut source,
            queue,
            self.prebuffer_target_ms,
        )
        .await?;
        if queue.is_empty() {
            return Err(PlaybackError::MediaParse("unexpected end of stream"));
        }

        self.current_video_id = Some(video_id.to_owned());
        Ok(source)
    }

    pub fn reset(&mut self) {
        self.current_video_id = None;
        self.recovery.reset();
        self.position = shared_playback_position(PlaybackPosition::default());
        self.normalizer = DiscordOpusFrameNormalizer::new();
        self.pending_frames.clear();
    }

    pub fn recovery_metrics(&self) -> PlaybackRecoveryMetrics {
        self.recovery.metrics()
    }

    pub fn set_prebuffer_target_ms(&mut self, target_ms: u64) {
        self.prebuffer_target_ms = target_ms.max(DEFAULT_PREBUFFER_TARGET_MS);
    }

    pub async fn fill_queue(
        &mut self,
        source: &mut PlaybackSource,
        queue: &mut OpusFrameQueue,
    ) -> Result<(), PlaybackError> {
        self.fill_queue_to_duration_ms(source, queue, self.prebuffer_target_ms)
            .await
    }

    pub async fn fill_queue_to_duration_ms(
        &mut self,
        source: &mut PlaybackSource,
        queue: &mut OpusFrameQueue,
        target_ms: u64,
    ) -> Result<(), PlaybackError> {
        let current_video_id = self.current_video_id.clone();
        self.fill_queue_to_duration_ms_for_video(
            current_video_id.as_deref(),
            source,
            queue,
            target_ms,
        )
        .await
    }

    async fn fill_queue_to_duration_ms_for_video(
        &mut self,
        video_id: Option<&str>,
        source: &mut PlaybackSource,
        queue: &mut OpusFrameQueue,
        target_ms: u64,
    ) -> Result<(), PlaybackError> {
        let target_ms = target_ms.max(1);
        let target_samples = samples_from_duration_ms_u64(target_ms);
        while queue.buffered_samples() < target_samples && !queue.is_full() {
            if !self.drain_pending_frames(queue)? {
                break;
            }
            if queue.buffered_samples() >= target_samples || queue.is_full() {
                break;
            }

            if let Some(packet) = source.pending_packets_mut().pop_front() {
                self.buffer_packet(queue, packet)?;
                continue;
            }

            let Some(chunk) = self.read_stream_chunk(video_id, source).await? else {
                let frames = self.normalizer.flush()?;
                self.enqueue_frames(queue, frames)?;
                break;
            };

            let packets = {
                let demux = source.demux_mut();
                demux.push_bytes(chunk);
                demux.drain_packets()?
            };
            tokio::task::yield_now().await;

            let mut packets = packets.into_iter();
            while let Some(packet) = packets.next() {
                if queue.buffered_samples() >= target_samples
                    || queue.is_full()
                    || !self.pending_frames.is_empty()
                {
                    source.pending_packets_mut().push_back(packet);
                    source.pending_packets_mut().extend(packets);
                    break;
                }
                self.buffer_packet(queue, packet)?;
            }
        }

        Ok(())
    }

    fn drain_pending_frames(&mut self, queue: &mut OpusFrameQueue) -> Result<bool, PlaybackError> {
        while let Some(frame) = self.pending_frames.pop_front() {
            if queue.can_fit_frame(&frame) {
                queue.push(frame).map_err(|_| PlaybackError::BufferFull)?;
            } else {
                self.pending_frames.push_front(frame);
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn enqueue_frames(
        &mut self,
        queue: &mut OpusFrameQueue,
        frames: Vec<OpusFrame>,
    ) -> Result<(), PlaybackError> {
        for frame in frames {
            if queue.can_fit_frame(&frame) {
                queue.push(frame).map_err(|_| PlaybackError::BufferFull)?;
            } else {
                self.pending_frames.push_back(frame);
            }
        }

        Ok(())
    }

    fn buffer_packet(
        &mut self,
        queue: &mut OpusFrameQueue,
        packet: DemuxedPacket,
    ) -> Result<(), PlaybackError> {
        let frames = self.normalizer.push_packet(packet.clone())?;
        self.position.lock().unwrap().record_buffered(&packet);
        self.enqueue_frames(queue, frames)
    }

    async fn read_stream_chunk(
        &mut self,
        video_id: Option<&str>,
        source: &mut PlaybackSource,
    ) -> Result<Option<bytes::Bytes>, PlaybackError> {
        self.recovery.read_stream_chunk(video_id, source).await
    }
}
