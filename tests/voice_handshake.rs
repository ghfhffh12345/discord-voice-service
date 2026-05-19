#[path = "support/fake_discord.rs"]
mod fake_discord;

use discord_voice_service::discord_voice::{handshake, session::ConnectedVoiceSession};

use self::fake_discord::FakeDiscordPeer;

#[tokio::test]
async fn connected_voice_session_does_not_require_synthetic_endpoint_query_params() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.is_connected(), "real-shape endpoint should connect");
}

#[tokio::test]
async fn voice_handshake_performs_identify_discovery_select_protocol_and_session_description() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "session-1", "token-1");

    let _session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(fake.saw_identify().await);
    assert!(fake.saw_select_protocol().await);
    assert!(fake.session_description_sent().await);
}

#[tokio::test]
async fn voice_handshake_can_resume_instead_of_identify() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "session-1", "token-1");

    let _session = handshake::resume(&voice, Some(42)).await.unwrap().unwrap();

    assert!(fake.saw_resume().await);
    assert!(!fake.saw_identify().await);
    assert!(fake.saw_select_protocol().await);
    assert!(fake.session_description_sent().await);
}
