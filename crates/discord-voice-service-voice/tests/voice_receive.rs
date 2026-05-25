use bytes::Bytes;
use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
use discord_voice_service_voice::ObservedVoiceSession;
use discord_voice_service_voice::dave::{
    DaveExternalSender, DaveMediaType, DaveRuntimeContext, DaveSession,
};
use tokio::net::UdpSocket;
use tokio::time::{Duration, Instant, sleep};

const CREATOR_USER_ID: &str = "1234123412341234";
const OBSERVER_USER_ID: &str = "5678567856785678";
const FAKE_DAVE_CREATOR_USER_ID: &str = "9999999999999999";

#[tokio::test]
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

#[tokio::test]
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

    assert!(error.to_string().contains("timed out"));
    assert!(
        elapsed < Duration::from_millis(300),
        "receive exceeded timeout budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn observed_voice_session_receives_and_dave_decrypts_audio_for_numeric_speaker() {
    let fake = FakeDiscordPeer::spawn_with_dave().await;
    let voice = fake.voice_context("1", "2", OBSERVER_USER_ID, "session-1", "token-1");

    let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
    assert!(fake.sent_dave_prepare_commit_transition().await);
    assert!(fake.saw_dave_init_transition_ready().await);
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
