use bytes::Bytes;
use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
use discord_voice_service_voice::{ConnectedVoiceSession, VoiceError};
use tokio::time::{Duration, sleep};

const BOT_USER_ID: &str = "1111111111111111";
const LATE_LISTENER_USER_ID: &str = "7777777777777777";
const GATEWAY_NOISE_COUNT_ABOVE_OLD_DRAIN_LIMIT: usize = 40;

#[tokio::test]
async fn connected_voice_session_sends_dave_audio_decryptable_by_existing_group_member() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session
        .send_audio_frame(Bytes::from(opus.clone()))
        .await
        .unwrap();

    assert_eq!(fake.audio_frame_count_at_least(1).await, 1);
    let decrypted = fake
        .decrypt_last_dave_audio_frame_from_creator(BOT_USER_ID)
        .await
        .unwrap();
    assert_eq!(decrypted, opus);
}

#[tokio::test]
async fn connected_voice_session_processes_late_dave_listener_transition_before_more_audio() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    let initial_opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session
        .send_audio_frame(Bytes::from(initial_opus.clone()))
        .await
        .unwrap();
    assert_eq!(
        fake.decrypt_last_dave_audio_frame_from_creator(BOT_USER_ID)
            .await
            .unwrap(),
        initial_opus
    );

    fake.inject_late_dave_listener_transition(LATE_LISTENER_USER_ID)
        .await
        .unwrap();
    sleep(Duration::from_millis(25)).await;
    let later_opus = hex::decode("f8b4011b2e11df489afb841af48c").unwrap();
    session
        .send_audio_frame(Bytes::from(later_opus.clone()))
        .await
        .unwrap();

    assert!(
        fake.saw_late_dave_transition_ready_within(Duration::from_millis(250))
            .await,
        "send-side DAVE handling must drain post-connect transition events before more media"
    );
    assert_eq!(fake.audio_frame_count_at_least(2).await, 2);
    let decrypted = fake
        .decrypt_last_dave_audio_frame_from_late_listener(BOT_USER_ID)
        .await
        .unwrap();
    assert_eq!(decrypted, later_opus);
}

#[tokio::test]
async fn connected_voice_session_processes_late_dave_transition_behind_gateway_noise() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    let initial_opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session
        .send_audio_frame(Bytes::from(initial_opus.clone()))
        .await
        .unwrap();
    assert_eq!(
        fake.decrypt_last_dave_audio_frame_from_creator(BOT_USER_ID)
            .await
            .unwrap(),
        initial_opus
    );

    fake.inject_late_dave_listener_transition_after_gateway_noise(
        LATE_LISTENER_USER_ID,
        GATEWAY_NOISE_COUNT_ABOVE_OLD_DRAIN_LIMIT,
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(25)).await;
    let later_opus = hex::decode("f8b4011b2e11df489afb841af48c").unwrap();
    session
        .send_audio_frame(Bytes::from(later_opus.clone()))
        .await
        .unwrap();

    assert!(
        fake.saw_late_dave_transition_ready_within(Duration::from_millis(250))
            .await,
        "send-side DAVE handling must not send stale-runtime media when DAVE is queued behind non-DAVE events"
    );
    assert_eq!(fake.audio_frame_count_at_least(2).await, 2);
    let decrypted = fake
        .decrypt_last_dave_audio_frame_from_late_listener(BOT_USER_ID)
        .await
        .unwrap();
    assert_eq!(decrypted, later_opus);
}

#[tokio::test]
async fn connected_voice_session_processes_late_dave_transition_when_prepare_epoch_arrives_after_proposals(
) {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    let initial_opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session
        .send_audio_frame(Bytes::from(initial_opus.clone()))
        .await
        .unwrap();
    assert_eq!(
        fake.decrypt_last_dave_audio_frame_from_creator(BOT_USER_ID)
            .await
            .unwrap(),
        initial_opus
    );

    fake.inject_late_dave_listener_transition_with_delayed_prepare_epoch(LATE_LISTENER_USER_ID)
        .await
        .unwrap();
    sleep(Duration::from_millis(25)).await;
    let later_opus = hex::decode("f8b4011b2e11df489afb841af48c").unwrap();
    session
        .send_audio_frame(Bytes::from(later_opus.clone()))
        .await
        .unwrap();

    assert!(
        fake.saw_late_dave_transition_ready_within(Duration::from_millis(250))
            .await,
        "send-side DAVE handling must recover when proposals arrive before prepare epoch"
    );
    assert_eq!(fake.audio_frame_count_at_least(2).await, 2);
    let decrypted = fake
        .decrypt_last_dave_audio_frame_from_late_listener(BOT_USER_ID)
        .await
        .unwrap();
    assert_eq!(decrypted, later_opus);
}

#[tokio::test]
async fn connected_voice_session_handles_unannounced_creator_before_new_group_proposals() {
    let fake = FakeDiscordPeer::spawn_with_dave_without_pre_announced_creator().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    sleep(Duration::from_millis(25)).await;
    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session.send_audio_frame(Bytes::from(opus.clone())).await.unwrap();

    assert!(fake.audio_frame_count_at_least(1).await >= 1);
}

#[tokio::test]
async fn connected_voice_session_fails_closed_while_late_dave_prepare_epoch_is_pending() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    let initial_opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session
        .send_audio_frame(Bytes::from(initial_opus.clone()))
        .await
        .unwrap();
    assert_eq!(fake.audio_frame_count_at_least(1).await, 1);

    fake.inject_late_dave_prepare_epoch_only().await.unwrap();
    sleep(Duration::from_millis(25)).await;
    let later_opus = hex::decode("f8b4011b2e11df489afb841af48c").unwrap();
    let err = session
        .send_audio_frame(Bytes::from(later_opus.clone()))
        .await
        .unwrap_err();

    assert_eq!(
        invalid_state_reason(err),
        "voice dave prepared transition pending"
    );
    assert_eq!(fake.audio_frame_count().await, 1);
}

#[tokio::test]
async fn connected_voice_session_fails_closed_after_invalid_late_dave_commit_transition() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    let initial_opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session
        .send_audio_frame(Bytes::from(initial_opus.clone()))
        .await
        .unwrap();
    assert_eq!(fake.audio_frame_count_at_least(1).await, 1);

    fake.inject_invalid_late_dave_prepare_commit()
        .await
        .unwrap();
    sleep(Duration::from_millis(25)).await;
    let later_opus = hex::decode("f8b4011b2e11df489afb841af48c").unwrap();
    let err = session
        .send_audio_frame(Bytes::from(later_opus.clone()))
        .await
        .unwrap_err();
    assert_eq!(invalid_state_reason(err), "voice dave commit invalid");
    assert_eq!(fake.audio_frame_count().await, 1);

    let retry_err = session
        .send_audio_frame(Bytes::from(later_opus))
        .await
        .unwrap_err();
    assert_eq!(
        invalid_state_reason(retry_err),
        "voice dave session failed closed"
    );
    assert_eq!(fake.audio_frame_count().await, 1);
}

#[tokio::test]
async fn connected_voice_session_ignores_replayed_established_join_commit_after_voice_ready() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", BOT_USER_ID, "session-1", "token-1");
    let mut session = ConnectedVoiceSession::connect(voice).await.unwrap();
    assert!(session.dave_enabled());

    let first = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    session.send_audio_frame(Bytes::from(first)).await.unwrap();

    fake.replay_established_join_commit_transition()
        .await
        .unwrap();
    sleep(Duration::from_millis(25)).await;

    let second = hex::decode("f8b4011b2e11df489afb841af48c").unwrap();
    session
        .send_audio_frame(Bytes::from(second.clone()))
        .await
        .unwrap();

    assert_eq!(fake.audio_frame_count_at_least(2).await, 2);
    assert_eq!(
        fake.decrypt_last_dave_audio_frame_from_creator(BOT_USER_ID)
            .await
            .unwrap(),
        second
    );
}

fn invalid_state_reason(err: VoiceError) -> &'static str {
    match err {
        VoiceError::InvalidState(reason) => reason,
        other => panic!("expected invalid state error, got {other:?}"),
    }
}
