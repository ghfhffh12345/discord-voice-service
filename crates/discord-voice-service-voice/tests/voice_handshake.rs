#[path = "support/fake_discord.rs"]
mod fake_discord;

use discord_voice_service_voice::{ConnectedVoiceSession, handshake};
use tokio::time::Duration;

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
async fn voice_handshake_starts_heartbeats_before_handshake_completes() {
    let fake =
        FakeDiscordPeer::spawn_real_shape_with_ready_delay(25, Duration::from_millis(100)).await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.is_connected());
    assert!(fake.heartbeat_count_at_least(1).await >= 1);
}

#[tokio::test]
async fn voice_session_completes_prepare_backed_dave_join() {
    let fake = FakeDiscordPeer::spawn_with_dave().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(fake.saw_dave_key_package_before_prepare_epoch().await);
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.sent_dave_prepare_commit_transition().await);
    assert!(
        !fake
            .saw_dave_transition_within(Duration::from_millis(100))
            .await
    );
    assert!(fake.saw_dave_init_transition_ready().await);
}

#[tokio::test]
async fn voice_session_sends_init_transition_ready_before_prepare_commit_transition_for_new_group_creator_path()
 {
    let fake =
        FakeDiscordPeer::spawn_with_dave_requiring_init_transition_ready_before_prepare_commit_transition()
            .await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.saw_dave_init_transition_ready().await);
    assert!(
        fake.saw_dave_init_transition_ready_before_prepare_commit_transition()
            .await
    );
    assert!(fake.sent_dave_prepare_commit_transition().await);
}

#[tokio::test]
async fn voice_session_sends_initial_key_package_before_external_sender_when_required() {
    let fake = FakeDiscordPeer::spawn_with_dave_requiring_init_key_package().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_key_package_before_external_sender().await);
    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.saw_dave_init_transition_ready().await);
}

#[tokio::test]
async fn voice_session_can_create_a_self_only_dave_group_without_proposals() {
    let fake = FakeDiscordPeer::spawn_with_dave_self_only_no_proposals().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_key_package_after_external_sender().await);
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.saw_dave_init_transition_ready().await);
    assert!(
        !fake
            .sent_dave_prepare_commit_transition_within(Duration::from_millis(100))
            .await
    );
    assert!(
        !fake
            .saw_dave_transition_within(Duration::from_millis(100))
            .await
    );
}

#[tokio::test]
async fn voice_session_refreshes_key_package_after_external_sender_when_required() {
    let fake = FakeDiscordPeer::spawn_with_dave_requiring_refreshed_key_package().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_key_package_before_external_sender().await);
    assert!(fake.saw_dave_key_package_after_external_sender().await);
    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.saw_dave_init_transition_ready().await);
}

#[tokio::test]
async fn voice_session_accepts_commit_transition_before_prepare_epoch_map_entry() {
    let fake = FakeDiscordPeer::spawn_with_dave_commit_before_prepare_epoch().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.sent_dave_prepare_commit_transition().await);
    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(fake.saw_dave_init_transition_ready().await);
}

#[tokio::test]
async fn voice_session_can_join_an_established_dave_group_without_prepare_epoch() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(
        !fake
            .saw_dave_prepare_epoch_within(Duration::from_millis(100))
            .await
    );
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
async fn voice_session_ignores_a_stray_dave_welcome_after_a_prepare_backed_welcome() {
    let fake = FakeDiscordPeer::spawn_with_prepare_backed_stray_dave_welcome().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(!fake.saw_unmatched_dave_transition().await);
    assert!(fake.saw_dave_transition().await);
}
