use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;

use discord_voice_service_voice::{ConnectedVoiceSession, handshake};
use tokio::time::Duration;

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
async fn voice_handshake_tolerates_clients_connect_before_session_description() {
    let fake =
        FakeDiscordPeer::spawn_real_shape_with_clients_connect_before_session_description().await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.is_connected());
    assert!(fake.saw_identify().await);
    assert!(fake.saw_select_protocol().await);
    assert!(fake.session_description_sent().await);
}

#[tokio::test]
async fn voice_handshake_tolerates_speaking_and_heartbeat_ack_before_session_description() {
    let fake =
        FakeDiscordPeer::spawn_real_shape_with_speaking_and_heartbeat_ack_before_session_description()
            .await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.is_connected());
    assert!(fake.saw_identify().await);
    assert!(fake.saw_select_protocol().await);
    assert!(fake.session_description_sent().await);
}

#[tokio::test]
async fn voice_handshake_rejects_self_disconnect_before_session_description() {
    let fake =
        FakeDiscordPeer::spawn_real_shape_with_self_disconnect_before_session_description().await;
    let voice = fake.voice_context("1", "2", "user-1", "session-1", "token-1");

    let error = ConnectedVoiceSession::connect(voice)
        .await
        .err()
        .expect("self disconnect should fail handshake");

    assert!(
        error
            .to_string()
            .contains("voice handshake session description missing")
    );
    assert!(fake.saw_identify().await);
    assert!(fake.saw_select_protocol().await);
    assert!(fake.session_description_sent().await);
}

#[tokio::test]
async fn voice_handshake_tolerates_non_self_disconnect_before_session_description() {
    let fake =
        FakeDiscordPeer::spawn_real_shape_with_self_disconnect_before_session_description().await;
    let voice = fake.voice_context("1", "2", "user-9", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.is_connected());
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
    assert!(fake.saw_dave_key_package_after_external_sender().await);
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.sent_dave_prepare_commit_transition().await);
    assert!(
        !fake
            .saw_dave_transition_within(Duration::from_millis(100))
            .await
    );
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
}

#[tokio::test]
async fn voice_session_ignores_no_op_dave_revoke_before_proposals() {
    let fake = FakeDiscordPeer::spawn_with_dave_no_op_revoke_before_proposals().await;
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
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
}

#[tokio::test]
async fn voice_session_does_not_send_init_transition_ready_for_new_group_creator_path() {
    let fake = FakeDiscordPeer::spawn_with_dave().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(fake.sent_dave_prepare_commit_transition().await);
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
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
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
}

#[tokio::test]
async fn voice_session_keeps_initial_dave_pending_without_proposals() {
    let fake = FakeDiscordPeer::spawn_with_dave_self_only_no_proposals().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_key_package_before_prepare_epoch().await);
    assert!(
        !fake
            .saw_dave_commit_welcome_within(Duration::from_millis(100))
            .await
    );
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
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
async fn voice_session_keeps_initial_dave_pending_when_recognized_peer_sends_no_proposals() {
    let fake = FakeDiscordPeer::spawn_with_dave_recognized_peer_no_proposals().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_key_package_before_prepare_epoch().await);
    assert!(
        !fake
            .saw_dave_commit_welcome_within(Duration::from_millis(100))
            .await
    );
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
}

#[tokio::test]
async fn voice_session_uses_initial_key_package_when_external_sender_arrives_afterwards() {
    let fake = FakeDiscordPeer::spawn_with_dave_requiring_init_key_package().await;
    let voice = fake.voice_context("1", "2", "1111111111111111", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_key_package_before_external_sender().await);
    assert!(fake.saw_dave_prepare_epoch().await);
    assert!(fake.saw_dave_commit_welcome().await);
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
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
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await
    );
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
    assert!(fake.saw_dave_key_package_after_external_sender().await);
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

#[tokio::test]
async fn observer_handshake_returns_without_waiting_for_delayed_established_join_material() {
    let fake = FakeDiscordPeer::spawn_with_delayed_established_dave_group_join().await;
    let voice = fake.voice_context("1", "2", "5678567856785678", "session-1", "token-1");

    let result: handshake::PendingObserverHandshakeResult = tokio::time::timeout(
        Duration::from_secs(1),
        handshake::connect_observer_participant(&voice),
    )
    .await
    .expect("observer handshake should return pending state before delayed join material")
    .expect("observer handshake should succeed")
    .expect("observer handshake should produce a connection");
    let dave = result
        .dave
        .expect("observer handshake should retain pending DAVE join state");

    assert!(
        dave.session.is_some(),
        "pending DAVE session should be preserved"
    );
    assert!(
        dave.pending_key_package,
        "pending DAVE join should retain key package state"
    );
    assert!(dave.pending_prepared_transitions.is_empty());
    assert!(dave.recognized_user_ids.contains("5678567856785678"));
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await,
        "observer handshake must not acknowledge transition 0 while join material is delayed"
    );
    assert!(
        !fake
            .saw_dave_commit_welcome_within(Duration::from_millis(100))
            .await,
        "observer handshake must not emit a local commit/welcome while waiting"
    );
}

#[tokio::test]
async fn observer_handshake_carries_negotiated_timeout_without_type_annotations() {
    let fake = FakeDiscordPeer::spawn_real_shape_with_heartbeat_interval(20_000).await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let result = handshake::connect_observer_participant(&voice)
        .await
        .unwrap()
        .expect("observer handshake connection");

    assert_eq!(result.dave_timeout, Duration::from_secs(40));
    assert!(result.dave.is_none());
}
