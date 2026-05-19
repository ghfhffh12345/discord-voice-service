#[path = "support/fake_discord.rs"]
mod fake_discord;

use discord_voice_service::discord_voice::{handshake, session::ConnectedVoiceSession};

use self::fake_discord::FakeDiscordPeer;

#[tokio::test]
async fn connected_voice_session_does_not_require_synthetic_endpoint_query_params() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.is_connected(), "real-shape endpoint should connect");
}

#[tokio::test]
async fn voice_handshake_performs_identify_discovery_select_protocol_and_session_description() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    let _session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(fake.saw_identify().await);
    assert!(fake.saw_select_protocol().await);
    assert!(fake.session_description_sent().await);
}

#[tokio::test]
async fn voice_handshake_can_resume_instead_of_identify() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    handshake::resume(&voice, Some(42)).await.unwrap();

    assert!(fake.saw_resume().await);
    assert!(!fake.saw_identify().await);
    assert!(!fake.saw_select_protocol().await);
    assert!(!fake.session_description_sent().await);
}

#[tokio::test]
async fn connected_voice_session_sends_periodic_heartbeats_after_hello() {
    let fake = FakeDiscordPeer::spawn_real_shape_with_heartbeat_interval(25).await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    let _session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(fake.heartbeat_count_at_least(1).await >= 1);
}

#[tokio::test]
async fn voice_session_completes_prepare_backed_dave_join() {
    let fake = FakeDiscordPeer::spawn_with_dave().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(!fake.saw_dave_key_package_before_prepare_epoch().await);
    assert!(fake.saw_dave_key_package_after_prepare_epoch().await);
    assert!(fake.saw_dave_transition().await);
}

#[tokio::test]
async fn voice_session_can_join_an_established_dave_group_without_prepare_epoch() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(!fake.saw_dave_prepare_epoch().await);
    assert!(fake.saw_dave_key_package_before_prepare_epoch().await);
    assert!(fake.saw_dave_transition().await);
}

#[tokio::test]
async fn voice_session_rejects_unmatched_dave_welcome_transition() {
    let fake = FakeDiscordPeer::spawn_with_unmatched_dave_welcome().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    assert!(ConnectedVoiceSession::connect(voice).await.is_err());
    assert!(!fake.saw_unmatched_dave_transition().await);
}

#[tokio::test]
async fn voice_session_does_not_acknowledge_a_stray_dave_welcome_after_a_prepare_backed_welcome() {
    let fake = FakeDiscordPeer::spawn_with_prepare_backed_stray_dave_welcome().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let error = match ConnectedVoiceSession::connect(voice).await {
        Ok(_) => panic!("stray welcome should not be accepted"),
        Err(error) => error,
    };

    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(!fake.saw_unmatched_dave_transition().await);
    assert_eq!(
        error.to_string(),
        "invalid state: voice dave welcome transition missing pending join"
    );
}
