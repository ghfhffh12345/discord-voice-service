#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFormat {
    pub itag: u32,
    pub mime_type: String,
    pub bitrate: u64,
    pub audio_sample_rate: Option<u32>,
    pub audio_channels: Option<u32>,
    pub has_video: bool,
}

impl StreamFormat {
    pub fn new(
        itag: u32,
        mime_type: &str,
        bitrate: u64,
        audio_sample_rate: Option<u32>,
        audio_channels: Option<u32>,
        has_video: bool,
    ) -> Self {
        Self {
            itag,
            mime_type: mime_type.to_owned(),
            bitrate,
            audio_sample_rate,
            audio_channels,
            has_video,
        }
    }
}

pub fn select_format(formats: &[StreamFormat]) -> Option<StreamFormat> {
    let allowed = formats
        .iter()
        .filter(|format| {
            !format.has_video
                && format.mime_type == "audio/webm; codecs=\"opus\""
                && format.audio_sample_rate == Some(48_000)
                && format.audio_channels == Some(2)
        })
        .cloned()
        .collect::<Vec<_>>();

    allowed
        .iter()
        .find(|format| format.itag == 250)
        .cloned()
        .or_else(|| allowed.iter().find(|format| format.itag == 249).cloned())
        .or_else(|| {
            allowed
                .iter()
                .filter(|format| format.bitrate < 50_000)
                .max_by_key(|format| format.bitrate)
                .cloned()
        })
}
