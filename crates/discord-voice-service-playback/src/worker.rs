use super::ytmusic_client::YtMusicClient;
use crate::error::PlaybackError;
use crate::media::opus_queue::{OpusFrame, OpusFrameQueue};
use crate::media::position::{PlaybackPosition, SharedPlaybackPosition, shared_playback_position};
use crate::media::webm_demux::DemuxedPacket;
use crate::recovery::{PlaybackRecovery, PlaybackRecoveryMetrics};
use crate::source::PlaybackSource;

const DEFAULT_PREBUFFER_TARGET_MS: u64 = 80;

pub struct PlaybackWorker {
    current_video_id: Option<String>,
    recovery: PlaybackRecovery,
    position: SharedPlaybackPosition,
    prebuffer_target_ms: u64,
}

impl PlaybackWorker {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            current_video_id: None,
            recovery: PlaybackRecovery::new(client),
            position: shared_playback_position(PlaybackPosition::default()),
            prebuffer_target_ms: DEFAULT_PREBUFFER_TARGET_MS,
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

        let resume_position_ms = if self.current_video_id.as_deref() == Some(video_id) {
            self.position.lock().unwrap().sent_duration_ms()
        } else {
            self.position = shared_playback_position(PlaybackPosition::default());
            0
        };

        let mut source = self.recovery.recover(video_id, resume_position_ms).await?;
        self.position = source.shared_position();

        self.fill_queue(&mut source, queue).await?;
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
        let target_ms = target_ms.max(1);
        while queue.buffered_duration_ms() < target_ms && !queue.is_full() {
            if let Some(packet) = source.pending_packets_mut().pop_front() {
                self.buffer_packet(queue, packet)?;
                continue;
            }

            let Some(chunk) = source.stream_mut().read_chunk().await? else {
                break;
            };

            let packets = {
                let demux = source.demux_mut();
                demux.push_bytes(chunk);
                demux.drain_packets()?
            };
            tokio::task::yield_now().await;

            for packet in packets {
                if queue.buffered_duration_ms() < target_ms && !queue.is_full() {
                    self.buffer_packet(queue, packet)?;
                } else {
                    source.pending_packets_mut().push_back(packet);
                }
            }
        }

        Ok(())
    }

    fn buffer_packet(
        &mut self,
        queue: &mut OpusFrameQueue,
        packet: DemuxedPacket,
    ) -> Result<(), PlaybackError> {
        self.position.lock().unwrap().record_buffered(&packet);
        let frame = OpusFrame::with_duration_samples(
            packet.data.clone(),
            packet.duration_ms,
            packet.duration_samples,
        )
        .with_metadata(packet.timestamp_ms, None, 0);
        queue.push(frame).map_err(|_| PlaybackError::BufferFull)
    }
}
