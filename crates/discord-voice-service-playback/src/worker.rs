use crate::error::PlaybackError;
use crate::media::opus_queue::{OpusFrame, OpusFrameQueue};
use crate::media::position::{PlaybackPosition, SharedPlaybackPosition, shared_playback_position};
use crate::media::webm_demux::DemuxedPacket;
use crate::recovery::PlaybackRecovery;
use crate::selector::select_song_stream_format;
use crate::source::PlaybackSource;
use crate::ytmusic_client::YtMusicClient;
use ytmusic_service_proto::ytmusic::v1::SongStreamFormat;

const DEFAULT_PREBUFFER_TARGET: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackPlan {
    pub video_id: String,
    pub selected_itag: u32,
}

impl PlaybackPlan {
    pub fn from_formats(video_id: &str, formats: &[SongStreamFormat]) -> Option<Self> {
        let selected = select_song_stream_format(formats).ok()?;
        Some(Self {
            video_id: video_id.to_owned(),
            selected_itag: selected.itag,
        })
    }
}

pub struct PlaybackWorker {
    current_video_id: Option<String>,
    recovery: PlaybackRecovery,
    position: SharedPlaybackPosition,
    prebuffer_target: usize,
}

impl PlaybackWorker {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            current_video_id: None,
            recovery: PlaybackRecovery::new(client),
            position: shared_playback_position(PlaybackPosition::default()),
            prebuffer_target: DEFAULT_PREBUFFER_TARGET,
        }
    }

    pub async fn prepare(
        &mut self,
        video_id: &str,
        queue: &mut OpusFrameQueue,
    ) -> Result<PlaybackSource, PlaybackError> {
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

    pub async fn fill_queue(
        &mut self,
        source: &mut PlaybackSource,
        queue: &mut OpusFrameQueue,
    ) -> Result<(), PlaybackError> {
        while queue.len() < self.prebuffer_target {
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

            for packet in packets {
                if queue.len() < self.prebuffer_target {
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
        queue
            .push(OpusFrame::new(packet.data.clone(), packet.duration_ms))
            .map_err(|_| PlaybackError::BufferFull)
    }
}
