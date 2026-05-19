#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;

use discord_voice_service::ytmusic::client::YtMusicClient;
use discord_voice_service::ytmusic::selector::select_song_stream_format;
use discord_voice_service::ytmusic::v1::SongStreamFormat;

use self::fake_ytmusic::FakeYtMusic;

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

#[tokio::test]
async fn resolve_playback_source_calls_get_song_and_decipher() {
    let fake = FakeYtMusic::spawn().await;
    let mut client = YtMusicClient::connect(fake.endpoint()).await.unwrap();

    let source = client.resolve_playback_source("video-1").await.unwrap();

    assert_eq!(source.selected_itag, 250);
    assert_eq!(source.playable_url, "https://cdn.example/audio.webm");
    assert_eq!(fake.calls(), vec!["GetSong", "Decipher"]);
}
