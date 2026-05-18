use discord_voice_service::config::Settings;
use discord_voice_service::error::AppError;

#[test]
fn parses_required_env_and_defaults() {
    let settings = Settings::from_pairs([
        ("DISCORD_VOICE_SERVICE_ADDR", "127.0.0.1:55051"),
        (
            "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR",
            "http://127.0.0.1:50051",
        ),
    ])
    .expect("settings should parse");

    assert_eq!(settings.listen_addr.to_string(), "127.0.0.1:55051");
    assert_eq!(settings.ytmusic_addr, "http://127.0.0.1:50051");
    assert_eq!(settings.prebuffer_frames, 150);
    assert_eq!(settings.max_buffer_frames, 300);
}

#[test]
fn rejects_invalid_ytmusic_endpoint() {
    let err = Settings::from_pairs([
        ("DISCORD_VOICE_SERVICE_ADDR", "127.0.0.1:55051"),
        ("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR", "not-a-grpc-endpoint"),
    ])
    .expect_err("settings should reject invalid ytmusic endpoint");

    assert!(matches!(
        err,
        AppError::InvalidEnv("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")
    ));
}
