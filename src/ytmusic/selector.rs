use std::cmp::Reverse;

use crate::error::AppError;
use crate::ytmusic::v1::SongStreamFormat;

pub fn select_song_stream_format(
    formats: &[SongStreamFormat],
) -> Result<SongStreamFormat, AppError> {
    let mut allowed = formats
        .iter()
        .filter(|format| {
            format.mime_type == "audio/webm; codecs=\"opus\""
                && format.audio_sample_rate == Some(48_000)
                && format.audio_channels == Some(2)
                && (matches!(format.itag, 250 | 249) || format.bitrate < 50_000)
        })
        .cloned()
        .collect::<Vec<_>>();

    allowed.sort_by_key(|format| (priority_for_itag(format.itag), Reverse(format.bitrate)));
    allowed.into_iter().next().ok_or(AppError::UnsupportedFormat)
}

fn priority_for_itag(itag: u32) -> u8 {
    match itag {
        250 => 0,
        249 => 1,
        _ => 2,
    }
}
