use discord_voice_service::discord_voice::dave::{
    DaveDecryptor, DaveEncryptor, DaveError, DaveExternalSender, DaveMediaType, DaveSession,
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
    assert_eq!(session_a.protocol_version(), 1);
    assert_eq!(session_b.protocol_version(), 1);

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
        commit_result.roster_member_ids(),
        vec![1234123412341234, 5678567856785678]
    );
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

    let ratchet_a = session_a.key_ratchet_for(user_a).expect("ratchet A");
    let ratchet_b = session_b.key_ratchet_for(user_a).expect("ratchet B");

    let mut encryptor = DaveEncryptor::new().expect("encryptor");
    encryptor.assign_opus_ssrc(0);
    encryptor
        .set_key_ratchet(&ratchet_a)
        .expect("encryptor ratchet");

    let mut decryptor = DaveDecryptor::new().expect("decryptor");
    decryptor
        .set_key_ratchet(&ratchet_b)
        .expect("decryptor ratchet");

    let audio_frame = hex::decode("0dc5aedd5bdc3f20be5697e54dd1f437").expect("hex audio");
    let encrypted = encryptor
        .encrypt(DaveMediaType::Audio, 0, &audio_frame)
        .expect("encrypt audio");
    assert_ne!(encrypted, audio_frame);

    let decrypted = decryptor
        .decrypt(DaveMediaType::Audio, &encrypted)
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
