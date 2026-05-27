use std::cmp::Reverse;

use crate::error::PlaybackError;
use ytmusic_service_client::v2::SongStreamFormat;

pub fn select_song_stream_format(
    formats: &[SongStreamFormat],
) -> Result<SongStreamFormat, PlaybackError> {
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
    allowed
        .into_iter()
        .next()
        .ok_or(PlaybackError::UnsupportedFormat)
}

fn priority_for_itag(itag: u32) -> u8 {
    match itag {
        250 => 0,
        249 => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::select_song_stream_format;
    use ytmusic_service_client::v2::SongStreamFormat;

    fn stream_format(
        itag: u32,
        mime_type: &str,
        bitrate: u64,
        audio_sample_rate: Option<u32>,
        audio_channels: Option<u32>,
    ) -> SongStreamFormat {
        SongStreamFormat {
            itag,
            mime_type: mime_type.to_owned(),
            bitrate,
            audio_sample_rate,
            audio_channels,
            signature_cipher: format!("cipher-{itag}"),
            ..Default::default()
        }
    }

    #[test]
    fn prefers_250_then_249_then_lower_only() {
        let formats = vec![
            stream_format(
                251,
                "audio/webm; codecs=\"opus\"",
                160_000,
                Some(48_000),
                Some(2),
            ),
            stream_format(
                250,
                "audio/webm; codecs=\"opus\"",
                70_000,
                Some(48_000),
                Some(2),
            ),
            stream_format(
                249,
                "audio/webm; codecs=\"opus\"",
                50_000,
                Some(48_000),
                Some(2),
            ),
        ];

        let selected = select_song_stream_format(&formats).expect("format should be selected");
        assert_eq!(selected.itag, 250);
    }

    #[test]
    fn falls_back_to_249_when_250_is_absent() {
        let formats = vec![
            stream_format(
                251,
                "audio/webm; codecs=\"opus\"",
                160_000,
                Some(48_000),
                Some(2),
            ),
            stream_format(
                249,
                "audio/webm; codecs=\"opus\"",
                50_000,
                Some(48_000),
                Some(2),
            ),
        ];

        let selected = select_song_stream_format(&formats).expect("format should be selected");
        assert_eq!(selected.itag, 249);
    }

    #[test]
    fn falls_back_to_lower_bitrate_webm_opus_when_250_and_249_are_absent() {
        let formats = vec![
            stream_format(
                251,
                "audio/webm; codecs=\"opus\"",
                160_000,
                Some(48_000),
                Some(2),
            ),
            stream_format(
                248,
                "audio/webm; codecs=\"opus\"",
                49_000,
                Some(48_000),
                Some(2),
            ),
            stream_format(
                247,
                "audio/webm; codecs=\"opus\"",
                40_000,
                Some(48_000),
                Some(2),
            ),
        ];

        let selected = select_song_stream_format(&formats).expect("format should be selected");
        assert_eq!(selected.itag, 248);
    }

    #[test]
    fn rejects_aac_and_video_formats() {
        let formats = vec![
            stream_format(
                140,
                "audio/mp4; codecs=\"mp4a.40.2\"",
                128_000,
                Some(44_100),
                Some(2),
            ),
            stream_format(160, "video/mp4; codecs=\"avc1\"", 90_000, None, None),
        ];

        assert!(select_song_stream_format(&formats).is_err());
    }
}
