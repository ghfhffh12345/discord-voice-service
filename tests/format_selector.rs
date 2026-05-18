use discord_voice_service::ytmusic::selector::{StreamFormat, select_format};

#[test]
fn prefers_250_then_249_then_lower_only() {
    let formats = vec![
        StreamFormat::new(
            251,
            "audio/webm; codecs=\"opus\"",
            160_000,
            Some(48_000),
            Some(2),
            false,
        ),
        StreamFormat::new(
            250,
            "audio/webm; codecs=\"opus\"",
            70_000,
            Some(48_000),
            Some(2),
            false,
        ),
        StreamFormat::new(
            249,
            "audio/webm; codecs=\"opus\"",
            50_000,
            Some(48_000),
            Some(2),
            false,
        ),
    ];

    let selected = select_format(&formats).expect("format should be selected");
    assert_eq!(selected.itag, 250);
}

#[test]
fn rejects_aac_and_video_formats() {
    let formats = vec![
        StreamFormat::new(
            140,
            "audio/mp4; codecs=\"mp4a.40.2\"",
            128_000,
            Some(44_100),
            Some(2),
            false,
        ),
        StreamFormat::new(160, "video/mp4; codecs=\"avc1\"", 90_000, None, None, true),
    ];

    assert!(select_format(&formats).is_none());
}
