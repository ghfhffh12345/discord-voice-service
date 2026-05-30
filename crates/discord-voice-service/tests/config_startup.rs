use discord_voice_service::config::Settings;
use discord_voice_service_runtime::RuntimeError;

#[test]
fn parses_required_env_and_defaults() {
    let settings = Settings::from_pairs([
        ("DISCORD_VOICE_SERVICE_BIND_ADDR", "127.0.0.1:55051"),
        (
            "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR",
            "http://127.0.0.1:50051",
        ),
    ])
    .expect("settings should parse");

    let Settings {
        bind_addr,
        ytmusic_addr,
    } = settings;

    assert_eq!(bind_addr.to_string(), "127.0.0.1:55051");
    assert_eq!(ytmusic_addr, "http://127.0.0.1:50051");
}

#[test]
fn rejects_legacy_bind_addr_variable_name() {
    let err = Settings::from_pairs([
        ("DISCORD_VOICE_SERVICE_ADDR", "127.0.0.1:55051"),
        (
            "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR",
            "http://127.0.0.1:50051",
        ),
    ])
    .expect_err("settings should reject the legacy bind address variable");

    assert!(matches!(
        err,
        RuntimeError::InvalidState("missing DISCORD_VOICE_SERVICE_BIND_ADDR")
    ));
}

#[test]
fn rejects_invalid_ytmusic_endpoint() {
    let err = Settings::from_pairs([
        ("DISCORD_VOICE_SERVICE_BIND_ADDR", "127.0.0.1:55051"),
        ("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR", "not-a-grpc-endpoint"),
    ])
    .expect_err("settings should reject invalid ytmusic endpoint");

    assert!(matches!(
        err,
        RuntimeError::InvalidState("invalid DISCORD_VOICE_SERVICE_YTMUSIC_ADDR")
    ));
}

#[test]
fn env_example_mentions_required_addresses() {
    let env_file = std::fs::read_to_string("../../.env.example").expect("env example");
    assert!(env_file.contains("DISCORD_VOICE_SERVICE_BIND_ADDR="));
    assert!(env_file.contains("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="));
    assert!(
        env_file.contains("YTMUSIC_SERVICE_IMAGE_REF=ghcr.io/ghfhffh12345/ytmusic-service:latest")
    );
    assert!(!env_file.contains("DISCORD_VOICE_SERVICE_ADDR="));
}
