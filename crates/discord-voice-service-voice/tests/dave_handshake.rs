use discord_voice_service_voice::dave::{
    DaveError, DaveExternalSender, DaveMediaType, DaveRuntimeContext, DaveSession,
};

#[test]
fn dave_handshake_establishes_matching_epoch_and_encrypts_audio() {
    let group_id = 1_234_567_890;
    let user_a = "1234123412341234";
    let user_b = "5678567856785678";

    let external_sender = DaveExternalSender::new(group_id).expect("external sender");
    let external_sender_bytes = external_sender
        .marshalled_external_sender()
        .expect("external sender bytes");

    let mut session_a = DaveSession::new(None).expect("session A");
    let mut session_b = DaveSession::new(None).expect("session B");

    session_a
        .set_external_sender(&external_sender_bytes)
        .expect("set sender A");
    session_b
        .set_external_sender(&external_sender_bytes)
        .expect("set sender B");

    session_a.init(1, group_id, user_a).expect("init A");
    session_b.init(1, group_id, user_b).expect("init B");

    let key_package_b = session_b.key_package().expect("key package B");
    let proposal = external_sender
        .propose_add(0, &key_package_b)
        .expect("add proposal");
    let recognized_user_ids = [user_a, user_b];
    let commit_welcome = session_a
        .process_proposals(&proposal, &recognized_user_ids)
        .expect("process proposals");
    let (commit, welcome) = external_sender
        .split_commit_welcome(&commit_welcome)
        .expect("split commit/welcome");

    let commit_result = session_a.process_commit(&commit).expect("process commit");
    let welcome_result = session_b
        .process_welcome(&welcome, &recognized_user_ids)
        .expect("process welcome");
    assert!(!commit_result.is_failed());
    assert!(!commit_result.is_ignored());
    assert_eq!(
        welcome_result.roster_member_ids(),
        commit_result.roster_member_ids()
    );

    let authenticator_a = session_a
        .last_epoch_authenticator()
        .expect("authenticator A");
    let authenticator_b = session_b
        .last_epoch_authenticator()
        .expect("authenticator B");
    assert_eq!(authenticator_a, authenticator_b);

    let fingerprint_a = session_a
        .pairwise_fingerprint(1, user_b)
        .expect("fingerprint A");
    let fingerprint_b = session_b
        .pairwise_fingerprint(1, user_a)
        .expect("fingerprint B");
    assert_eq!(fingerprint_a, fingerprint_b);

    let audio_frame = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").expect("hex audio");
    let encrypted = session_a
        .encrypt_audio_frame(&audio_frame)
        .expect("encrypt audio");
    let decrypted = session_b
        .decrypt_audio_frame_from(user_a, DaveMediaType::Audio, &encrypted)
        .expect("decrypt audio");
    assert_eq!(decrypted, audio_frame);
}

#[test]
fn dave_session_surfaces_mls_failure_callback_diagnostics() {
    let mut session = DaveSession::new(None).expect("session");

    let err = session
        .set_external_sender(&[0xff])
        .expect_err("invalid external sender should fail");

    match err {
        DaveError::MlsFailure {
            context,
            mls_source,
            reason,
        } => {
            assert_eq!(context, "set external sender");
            assert_eq!(mls_source, "SetExternalSender");
            assert!(!reason.is_empty());
        }
        other => panic!("expected MLS failure diagnostics, got {other:?}"),
    }
}

#[test]
fn dave_empty_append_proposals_do_not_create_self_only_commit() {
    const GROUP_ID: u64 = 1_234_567_890;
    const PROTOCOL_VERSION: u16 = 1;
    const CREATOR_USER_ID: &str = "1234123412341234";

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

    let err = creator
        .process_proposals(&[0], &[CREATOR_USER_ID])
        .expect_err("empty append proposals should not synthesize a commit");

    assert!(err.to_string().contains("append proposals"));
}

#[test]
fn dave_runtime_context_builds_send_side_encrypt_state_from_welcomed_session() {
    const GROUP_ID: u64 = 1_234_567_890;
    const PROTOCOL_VERSION: u16 = 1;
    const CREATOR_USER_ID: &str = "1234123412341234";
    const RUNTIME_USER_ID: &str = "5678567856785678";

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

    let mut runtime_member = DaveSession::new(None).expect("runtime member session");
    runtime_member
        .set_external_sender(&external_sender_bytes)
        .expect("runtime external sender");
    runtime_member
        .init(PROTOCOL_VERSION, GROUP_ID, RUNTIME_USER_ID)
        .expect("runtime init");

    let key_package = runtime_member.key_package().expect("runtime key package");
    let proposal = external_sender
        .propose_add(0, &key_package)
        .expect("add proposal");
    let recognized_user_ids = [CREATOR_USER_ID, RUNTIME_USER_ID];
    let commit_welcome = creator
        .process_proposals(&proposal, &recognized_user_ids)
        .expect("creator process proposals");
    let (commit, welcome) = external_sender
        .split_commit_welcome(&commit_welcome)
        .expect("split commit/welcome");
    creator
        .process_commit(&commit)
        .expect("creator process commit");
    runtime_member
        .process_welcome(&welcome, &recognized_user_ids)
        .expect("runtime welcome");

    let mut runtime = DaveRuntimeContext::from_session(runtime_member).expect("runtime context");

    let local_audio = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").expect("local audio");
    let local_encrypted = runtime
        .encrypt_audio_frame(&local_audio)
        .expect("runtime encrypt");
    let local_decrypted = creator
        .decrypt_audio_frame_from(RUNTIME_USER_ID, DaveMediaType::Audio, &local_encrypted)
        .expect("creator decrypt");
    assert_eq!(local_decrypted, local_audio);
}
