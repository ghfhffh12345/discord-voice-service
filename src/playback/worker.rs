use crate::ytmusic::selector::{select_format, StreamFormat};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackPlan {
    pub video_id: String,
    pub selected_itag: u32,
}

impl PlaybackPlan {
    pub fn from_formats(video_id: &str, formats: &[StreamFormat]) -> Option<Self> {
        let selected = select_format(formats)?;
        Some(Self {
            video_id: video_id.to_owned(),
            selected_itag: selected.itag,
        })
    }
}
