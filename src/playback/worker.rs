use crate::error::AppError;
use crate::media::opus_queue::{OpusFrame, OpusFrameQueue};
use crate::media::position::PlaybackPosition;
use crate::media::webm_demux::DemuxedPacket;
use crate::playback::recovery::PlaybackRecovery;
use crate::playback::source::PlaybackSource;
use crate::ytmusic::client::YtMusicClient;
use crate::ytmusic::selector::select_song_stream_format;
use crate::ytmusic::v1::SongStreamFormat;

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
    recovery: PlaybackRecovery,
    position: PlaybackPosition,
    prebuffer_target: usize,
}

impl PlaybackWorker {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            recovery: PlaybackRecovery::new(client),
            position: PlaybackPosition::default(),
            prebuffer_target: DEFAULT_PREBUFFER_TARGET,
        }
    }

    pub async fn prepare(
        &mut self,
        video_id: &str,
        queue: &mut OpusFrameQueue,
    ) -> Result<PlaybackSource, AppError> {
        self.position = PlaybackPosition::default();

        let mut source = self.recovery.recover(video_id, 0).await?;

        while queue.len() < self.prebuffer_target {
            if let Some(packet) = source.pending_packets_mut().pop_front() {
                self.buffer_packet(queue, packet)?;
                continue;
            }

            let Some(chunk) = source.stream_mut().read_chunk().await? else {
                return Err(AppError::MediaParse("unexpected end of stream"));
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

        self.position = source.position();
        Ok(source)
    }

    fn buffer_packet(
        &mut self,
        queue: &mut OpusFrameQueue,
        packet: DemuxedPacket,
    ) -> Result<(), AppError> {
        self.position.record_buffered(&packet);
        queue
            .push(OpusFrame::new(packet.data.clone(), packet.duration_ms))
            .map_err(|_| AppError::BufferFull)
    }
}
