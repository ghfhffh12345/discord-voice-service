use std::future::{Future, poll_fn};
use std::task::Poll;

use bytes::Bytes;
use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
use discord_voice_service_voice::dave::{
    DaveExternalSender, DaveMediaType, DaveRuntimeContext, DaveSession,
};
use discord_voice_service_voice::test_support::RtpPacketBuilder;
use discord_voice_service_voice::{
    ObservedVoiceActivity, ObservedVoiceSession, PendingObservedVoiceSession,
};
use tokio::net::UdpSocket;
use tokio::time::{Duration, Instant, sleep};

const CREATOR_USER_ID: &str = "1234123412341234";
const OBSERVER_USER_ID: &str = "5678567856785678";
const FAKE_DAVE_CREATOR_USER_ID: &str = "9999999999999999";
const FAKE_DAVE_FOREIGN_USER_ID: &str = "7777777777777777";
// Match the production post-HELLO DAVE floor for slower hosted runners.
const DAVE_READY_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_receives_protected_audio_and_resolves_speaker_from_gateway() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    let foreign = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    foreign
        .send_to(b"stray-packet", fake.last_udp_peer().await.unwrap())
        .await
        .unwrap();
    fake.send_speaking("speaker-1", 42).await.unwrap();
    fake.send_protected_audio_packet(42, b"opus-frame")
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame(Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, "speaker-1");
    assert_eq!(frame.ssrc, 42);
    assert_eq!(frame.payload, Bytes::from_static(b"opus-frame"));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_resolves_speaking_zero_without_user_id_by_ssrc() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    fake.send_speaking("speaker-1", 42).await.unwrap();
    let started = session
        .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(started.ssrc, 42);

    fake.send_speaking_state_without_user(0, 42).await.unwrap();
    let stopped = session
        .receive_speaking_state_from("speaker-1", 0, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(stopped.user_id, "speaker-1");
    assert_eq!(stopped.ssrc, 42);
    assert_eq!(stopped.speaking, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_treats_no_indicator_state_as_not_microphone_speaking() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    fake.send_speaking("speaker-1", 42).await.unwrap();
    session
        .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
        .await
        .unwrap();

    fake.send_speaking_state_without_user(2, 42).await.unwrap();
    let stopped = session
        .receive_non_microphone_speaking_state_from("speaker-1", Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(stopped.user_id, "speaker-1");
    assert_eq!(stopped.ssrc, 42);
    assert_eq!(stopped.speaking, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_activity_reports_speaking_and_audio() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    fake.send_speaking("speaker-1", 42).await.unwrap();

    let activity = session
        .receive_activity_from("speaker-1", Duration::from_secs(1))
        .await
        .unwrap();
    match activity {
        ObservedVoiceActivity::Speaking(state) => {
            assert_eq!(state.user_id, "speaker-1");
            assert_eq!(state.ssrc, 42);
            assert_eq!(state.speaking, 1);
        }
        ObservedVoiceActivity::Audio(frame) => {
            panic!("expected speaking state before audio, got {frame:?}");
        }
        ObservedVoiceActivity::RtpPacket(packet) => {
            panic!("expected speaking state before audio, got RTP packet {packet:?}");
        }
        ObservedVoiceActivity::Disconnect(user_id) => {
            panic!("expected speaking state before audio, got disconnect for {user_id}");
        }
    }

    fake.send_protected_audio_packet(42, b"opus-frame")
        .await
        .unwrap();
    let activity = session
        .receive_activity_from("speaker-1", Duration::from_secs(1))
        .await
        .unwrap();

    match activity {
        ObservedVoiceActivity::Audio(frame) => {
            assert_eq!(frame.user_id, "speaker-1");
            assert_eq!(frame.ssrc, 42);
            assert_eq!(frame.payload, Bytes::from_static(b"opus-frame"));
        }
        ObservedVoiceActivity::Speaking(state) => {
            panic!("expected audio frame after speaking state, got {state:?}");
        }
        ObservedVoiceActivity::RtpPacket(packet) => {
            panic!("expected audio frame after speaking state, got RTP packet {packet:?}");
        }
        ObservedVoiceActivity::Disconnect(user_id) => {
            panic!("expected audio frame after speaking state, got disconnect for {user_id}");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_activity_reports_client_disconnect() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    fake.send_client_disconnect("speaker-1").await.unwrap();

    let activity = session
        .receive_activity_from("speaker-1", Duration::from_secs(1))
        .await
        .unwrap();

    match activity {
        ObservedVoiceActivity::Disconnect(user_id) => {
            assert_eq!(user_id, "speaker-1");
        }
        ObservedVoiceActivity::Audio(frame) => {
            panic!("expected disconnect, got audio frame {frame:?}");
        }
        ObservedVoiceActivity::RtpPacket(packet) => {
            panic!("expected disconnect, got RTP packet {packet:?}");
        }
        ObservedVoiceActivity::Speaking(state) => {
            panic!("expected disconnect, got speaking state {state:?}");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_times_out_when_speaker_mapping_never_arrives() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    tokio::spawn({
        let fake = fake;
        async move {
            sleep(Duration::from_millis(150)).await;
            fake.send_protected_audio_packet(42, b"still-encrypted")
                .await
                .unwrap();
        }
    });

    let start = Instant::now();
    let error = session
        .receive_audio_frame(Duration::from_millis(200))
        .await
        .unwrap_err();
    let elapsed = start.elapsed();

    assert!(
        error.to_string().contains("timed out"),
        "unexpected error: {error}"
    );
    assert!(
        elapsed < Duration::from_millis(300),
        "receive exceeded timeout budget: {elapsed:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_ignores_unknown_ssrc_packet_before_target_audio() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    tokio::spawn({
        let fake = fake;
        async move {
            fake.send_protected_audio_packet(41, b"wrong-ssrc-frame")
                .await
                .unwrap();
            sleep(Duration::from_millis(50)).await;
            fake.send_speaking("speaker-1", 42).await.unwrap();
            sleep(Duration::from_millis(10)).await;
            fake.send_protected_audio_packet(42, b"opus-frame")
                .await
                .unwrap();
        }
    });

    let frame = session
        .receive_audio_frame_from("speaker-1", Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, "speaker-1");
    assert_eq!(frame.ssrc, 42);
    assert_eq!(frame.payload, Bytes::from_static(b"opus-frame"));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_receives_and_dave_decrypts_audio_for_numeric_speaker() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    assert!(fake.saw_dave_transition().await);
    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame(Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_resolves_unknown_ssrc_by_expected_dave_speaker_decrypt() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    assert!(fake.saw_dave_transition().await);
    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.ssrc, 42);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_ignores_unprotectable_unknown_ssrc_before_target_dave_audio() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    assert!(fake.saw_dave_transition().await);

    let malformed = RtpPacketBuilder::new(77).build(0, 0, b"not-a-valid-protected-packet");
    fake.send_raw_udp_packet(&malformed).await.unwrap();

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.ssrc, 42);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_keeps_unprotectable_mapped_target_packet_fatal() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    assert!(fake.saw_dave_transition().await);

    session.record_speaker_ssrc(FAKE_DAVE_CREATOR_USER_ID, 77);
    let malformed = RtpPacketBuilder::new(77).build(0, 0, b"not-a-valid-protected-packet");
    fake.send_raw_udp_packet(&malformed).await.unwrap();

    let error = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap_err();

    assert!(error.is_packet_unprotect_failure());
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_ignores_foreign_dave_speaker_before_target_audio() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    assert!(fake.saw_dave_transition().await);
    fake.send_speaking(FAKE_DAVE_FOREIGN_USER_ID, 41)
        .await
        .unwrap();
    fake.send_protected_audio_packet(41, b"not-a-dave-frame")
        .await
        .unwrap();

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_receives_audio_after_replayed_established_join_welcome() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();

    fake.replay_established_join_welcome_transition()
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_stays_decrypt_compatible_after_post_join_remote_commit() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    let receive_task = tokio::spawn(async move {
        session
            .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(2))
            .await
    });

    fake.inject_remote_observer_post_join_commit("3333333333333333")
        .await
        .unwrap();
    assert!(
        fake.saw_late_dave_transition_ready_within(DAVE_READY_TIMEOUT)
            .await,
        "observer should acknowledge active post-join DAVE transitions"
    );
    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = receive_task.await.unwrap().unwrap();
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_reinitializes_after_stale_authored_proposals() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    let receive_task = tokio::spawn(async move {
        session
            .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(3))
            .await
    });

    fake.inject_stale_dave_proposal("3333333333333333")
        .await
        .unwrap();
    assert!(
        fake.saw_dave_invalid_commit_welcome_within(0, DAVE_READY_TIMEOUT)
            .await,
        "observer should signal stale DAVE proposal processing"
    );
    assert!(
        fake.saw_dave_key_package_count_at_least(2, DAVE_READY_TIMEOUT)
            .await,
        "observer should send a fresh key package after stale proposals"
    );

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = receive_task.await.unwrap().unwrap();
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_can_leave_post_join_proposals_to_active_sender() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    session.set_dave_proposal_authoring(false);

    let receive_task = tokio::spawn(async move {
        session
            .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(2))
            .await
    });

    fake.inject_late_dave_listener_transition("3333333333333333")
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;
    assert!(
        !fake
            .saw_dave_commit_welcome_within(Duration::from_millis(50))
            .await,
        "passive observer must not author post-join DAVE proposal commits"
    );

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = receive_task.await.unwrap().unwrap();
    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn pending_observed_voice_session_can_leave_initial_proposals_to_active_sender() {
    let fake =
        FakeDiscordPeer::spawn_with_delayed_established_dave_group_join_after_proposals().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let mut pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    pending.set_dave_proposal_authoring(false);
    let ready = pending.await_dave_ready(DAVE_READY_TIMEOUT);
    tokio::pin!(ready);

    assert!(fake.saw_dave_key_package_after_external_sender().await);
    sleep(Duration::from_millis(100)).await;
    assert!(
        !fake
            .saw_dave_commit_welcome_within(Duration::from_millis(50))
            .await,
        "passive pending observer must not author initial DAVE proposal commits"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut ready)
            .await
            .is_err(),
        "observer join should wait for the sender-authored initial transition"
    );

    fake.release_delayed_established_join_material()
        .await
        .unwrap();
    let mut session = ready.await.unwrap();

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_rejects_unmatched_dave_welcome_transition() {
    let fake = FakeDiscordPeer::spawn_with_unmatched_dave_welcome().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    assert!(
        pending
            .await_dave_ready(Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(!fake.saw_unmatched_dave_transition().await);
}

#[tokio::test(flavor = "current_thread")]
async fn pending_observed_voice_session_waits_for_delayed_established_join_material_before_ready() {
    let fake = FakeDiscordPeer::spawn_with_delayed_established_dave_group_join().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let ready = pending.await_dave_ready(DAVE_READY_TIMEOUT);
    tokio::pin!(ready);
    poll_fn(|cx| match ready.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("observer join should block until delayed material is released"),
    })
    .await;

    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await,
        "observer join must not acknowledge init transition 0 while existing-group material is delayed"
    );
    assert!(
        !fake
            .saw_dave_commit_welcome_within(Duration::from_millis(100))
            .await,
        "observer join must not emit a local commit/welcome while waiting"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut ready)
            .await
            .is_err(),
        "observer join should still be waiting"
    );

    fake.release_delayed_established_join_material()
        .await
        .unwrap();
    let mut session = ready.await.unwrap();

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn pending_observed_voice_session_authors_local_commit_and_receives_audio_without_gateway_prepare()
 {
    let fake =
        FakeDiscordPeer::spawn_with_dave_recognized_peer_queued_init_prepare_commit_until_control()
            .await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    assert!(
        fake.saw_dave_commit_welcome_within(Duration::from_millis(100))
            .await,
        "observer join should author a local commit/welcome from proposals"
    );
    assert!(
        !fake
            .saw_dave_init_transition_ready_within(Duration::from_millis(100))
            .await,
        "observer join must not acknowledge init transition 0"
    );

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn pending_observed_voice_session_preserves_speaking_state_consumed_before_ready() {
    let fake = FakeDiscordPeer::spawn_with_delayed_established_dave_group_join().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let ready = pending.await_dave_ready(DAVE_READY_TIMEOUT);
    tokio::pin!(ready);
    poll_fn(|cx| match ready.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("observer join should still be pending before delayed material"),
    })
    .await;
    assert!(fake.saw_dave_key_package_after_external_sender().await);

    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.release_delayed_established_join_material()
        .await
        .unwrap();
    let mut session = ready.await.unwrap();

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn pending_observed_voice_session_preserves_seed_phase_speaking_state_before_connect_returns()
{
    let fake = FakeDiscordPeer::spawn_with_delayed_established_dave_group_join().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let connect = tokio::spawn(async move { PendingObservedVoiceSession::connect(voice).await });
    assert!(fake.session_description_sent().await);
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    let pending = connect.await.unwrap().unwrap();
    let ready = pending.await_dave_ready(DAVE_READY_TIMEOUT);
    tokio::pin!(ready);
    poll_fn(|cx| match ready.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("observer join should still be pending before delayed material"),
    })
    .await;
    assert!(fake.saw_dave_key_package_after_external_sender().await);

    fake.release_delayed_established_join_material()
        .await
        .unwrap();
    let mut session = ready.await.unwrap();

    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_receives_audio_after_replayed_established_join_commit() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();

    fake.replay_established_join_commit_transition()
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_voice_session_receives_audio_after_real_prepare_epoch_without_transition_id() {
    let fake = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let pending = PendingObservedVoiceSession::connect(voice).await.unwrap();
    let mut session = pending.await_dave_ready(DAVE_READY_TIMEOUT).await.unwrap();
    let opus = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").unwrap();
    let encrypted = fake
        .encrypt_dave_audio_frame_from_creator(&opus)
        .await
        .unwrap();

    fake.inject_observer_prepare_epoch_without_transition_id()
        .await
        .unwrap();
    fake.send_speaking(FAKE_DAVE_CREATOR_USER_ID, 42)
        .await
        .unwrap();
    fake.send_protected_audio_packet(42, &encrypted)
        .await
        .unwrap();

    let frame = session
        .receive_audio_frame_from(FAKE_DAVE_CREATOR_USER_ID, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(frame.user_id, FAKE_DAVE_CREATOR_USER_ID);
    assert_eq!(frame.payload, Bytes::from(opus));
}

#[test]
fn dave_runtime_context_decrypts_audio_frame_from_sender_user() {
    const GROUP_ID: u64 = 1_234_567_890;
    const PROTOCOL_VERSION: u16 = 1;

    let external_sender = DaveExternalSender::new(GROUP_ID).expect("external sender");
    let external_sender_bytes = external_sender
        .marshalled_external_sender()
        .expect("external sender bytes");

    let mut creator = DaveSession::new(None).expect("creator session");
    creator
        .set_external_sender(&external_sender_bytes)
        .expect("creator external sender");
    creator
        .init(PROTOCOL_VERSION, GROUP_ID, CREATOR_USER_ID)
        .expect("creator init");

    let mut observer = DaveSession::new(None).expect("observer session");
    observer
        .set_external_sender(&external_sender_bytes)
        .expect("observer external sender");
    observer
        .init(PROTOCOL_VERSION, GROUP_ID, OBSERVER_USER_ID)
        .expect("observer init");

    let key_package = observer.key_package().expect("observer key package");
    let proposal = external_sender
        .propose_add(0, &key_package)
        .expect("add proposal");
    let recognized_user_ids = [CREATOR_USER_ID, OBSERVER_USER_ID];
    let commit_welcome = creator
        .process_proposals(&proposal, &recognized_user_ids)
        .expect("creator process proposals");
    let (commit, welcome) = external_sender
        .split_commit_welcome(&commit_welcome)
        .expect("split commit/welcome");
    creator
        .process_commit(&commit)
        .expect("creator process commit");
    observer
        .process_welcome(&welcome, &recognized_user_ids)
        .expect("observer welcome");

    let mut runtime = DaveRuntimeContext::from_session(observer).expect("runtime context");
    let audio_frame = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").expect("audio frame");
    let encrypted = creator
        .encrypt_audio_frame(&audio_frame)
        .expect("creator encrypt");

    let decrypted = runtime
        .decrypt_audio_frame_from(CREATOR_USER_ID, DaveMediaType::Audio, &encrypted)
        .expect("runtime decrypt");

    assert_eq!(decrypted, audio_frame);
}
