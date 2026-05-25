#![allow(dead_code)]

use std::sync::{Arc, Mutex as StdMutex};

use discord_voice_service_voice::VoiceContext;
use discord_voice_service_voice::crypto::{PREFERRED_MODE, REQUIRED_MODE};
use discord_voice_service_voice::dave::{DaveExternalSender, DaveSession};
use discord_voice_service_voice::test_support::split_dave_mls_commit_welcome_payload;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

const DAVE_CREATOR_USER_ID: &str = "9999999999999999";
const DAVE_EXISTING_MEMBER_USER_ID: &str = "8888888888888888";
const DAVE_PROTOCOL_VERSION: u16 = 1;
const DAVE_INIT_TRANSITION_ID: u16 = 0;
const DAVE_TRANSITION_ID: u16 = 1;
const DAVE_UNMATCHED_TRANSITION_ID: u16 = 9;

#[derive(Clone, Copy, PartialEq, Eq)]
enum DaveScenario {
    Disabled,
    NewGroup,
    NewGroupWithNoOpRevokeBeforeProposals,
    NewGroupSelfOnlyNoProposals,
    NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition,
    NewGroupCommitBeforePrepareEpoch,
    NewGroupRequiresInitKeyPackage,
    NewGroupRequiresRefreshedKeyPackage,
    EstablishedGroupJoin,
    PrepareBackedWelcomeWithStrayFollowUp,
    UnmatchedWelcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PreSessionDescriptionEvent {
    ClientsConnect(Vec<String>),
    ClientDisconnect(String),
    Speaking { user_id: String, ssrc: u32 },
    HeartbeatAck(Option<u64>),
}

pub struct FakeDiscordPeer {
    endpoint_host: String,
    gateway_path: Arc<StdMutex<Option<String>>>,
    dave_group_id: Arc<StdMutex<Option<u64>>>,
    discovery_count: Arc<Mutex<usize>>,
    speaking_observed: Arc<Notify>,
    audio_frame_count: Arc<Mutex<usize>>,
    audio_frame_times: Arc<Mutex<Vec<Instant>>>,
    heartbeat_count: Arc<Mutex<usize>>,
    saw_identify: Arc<Mutex<bool>>,
    saw_resume: Arc<Mutex<bool>>,
    saw_select_protocol: Arc<Mutex<bool>>,
    session_description_sent: Arc<Mutex<bool>>,
    saw_dave_transition: Arc<Mutex<bool>>,
    saw_unmatched_dave_transition: Arc<Mutex<bool>>,
    saw_dave_prepare_epoch: Arc<Mutex<bool>>,
    saw_dave_key_package_before_prepare_epoch: Arc<Mutex<bool>>,
    saw_dave_key_package_after_prepare_epoch: Arc<Mutex<bool>>,
    saw_dave_key_package_before_external_sender: Arc<Mutex<bool>>,
    saw_dave_key_package_after_external_sender: Arc<Mutex<bool>>,
    saw_dave_commit_welcome: Arc<Mutex<bool>>,
    saw_dave_init_transition_ready: Arc<Mutex<bool>>,
    saw_dave_init_transition_ready_before_prepare_commit_transition: Arc<Mutex<bool>>,
    sent_dave_prepare_commit_transition: Arc<Mutex<bool>>,
}

impl FakeDiscordPeer {
    #[allow(clippy::result_large_err)]
    pub async fn spawn() -> Self {
        Self::spawn_real_shape().await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape() -> Self {
        Self::spawn_with_options(1_000, DaveScenario::Disabled, Duration::ZERO, vec![]).await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape_with_heartbeat_interval(heartbeat_interval_ms: u64) -> Self {
        Self::spawn_with_options_and_ready_delay(
            heartbeat_interval_ms,
            DaveScenario::Disabled,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_gateway_delay(delay: Duration) -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::Disabled,
            delay,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape_with_ready_delay(
        heartbeat_interval_ms: u64,
        ready_delay: Duration,
    ) -> Self {
        Self::spawn_with_options_and_ready_delay(
            heartbeat_interval_ms,
            DaveScenario::Disabled,
            Duration::ZERO,
            ready_delay,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape_with_clients_connect_before_session_description() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::Disabled,
            Duration::ZERO,
            Duration::ZERO,
            vec![PreSessionDescriptionEvent::ClientsConnect(vec![
                "user-2".to_owned(),
            ])],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape_with_speaking_and_heartbeat_ack_before_session_description()
    -> Self {
        Self::spawn_with_options_and_ready_delay(
            25,
            DaveScenario::Disabled,
            Duration::ZERO,
            Duration::from_millis(100),
            vec![
                PreSessionDescriptionEvent::Speaking {
                    user_id: "user-2".to_owned(),
                    ssrc: 42,
                },
                PreSessionDescriptionEvent::HeartbeatAck(Some(7)),
            ],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_real_shape_with_self_disconnect_before_session_description() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::Disabled,
            Duration::ZERO,
            Duration::ZERO,
            vec![PreSessionDescriptionEvent::ClientDisconnect(
                "user-1".to_owned(),
            )],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_dave() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::NewGroup,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_dave_no_op_revoke_before_proposals() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::NewGroupWithNoOpRevokeBeforeProposals,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_dave_self_only_no_proposals() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::NewGroupSelfOnlyNoProposals,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_dave_requiring_init_transition_ready_before_prepare_commit_transition()
    -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_dave_commit_before_prepare_epoch() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::NewGroupCommitBeforePrepareEpoch,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_dave_requiring_init_key_package() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::NewGroupRequiresInitKeyPackage,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_dave_requiring_refreshed_key_package() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::NewGroupRequiresRefreshedKeyPackage,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_established_dave_group() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::EstablishedGroupJoin,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_unmatched_dave_welcome() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::UnmatchedWelcome,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn spawn_with_prepare_backed_stray_dave_welcome() -> Self {
        Self::spawn_with_options_and_ready_delay(
            1_000,
            DaveScenario::PrepareBackedWelcomeWithStrayFollowUp,
            Duration::ZERO,
            Duration::ZERO,
            vec![],
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    async fn spawn_with_options(
        heartbeat_interval_ms: u64,
        dave_scenario: DaveScenario,
        gateway_delay: Duration,
        pre_session_description_events: Vec<PreSessionDescriptionEvent>,
    ) -> Self {
        Self::spawn_with_options_and_ready_delay(
            heartbeat_interval_ms,
            dave_scenario,
            gateway_delay,
            Duration::ZERO,
            pre_session_description_events,
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    async fn spawn_with_options_and_ready_delay(
        heartbeat_interval_ms: u64,
        dave_scenario: DaveScenario,
        gateway_delay: Duration,
        ready_delay: Duration,
        pre_session_description_events: Vec<PreSessionDescriptionEvent>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        let gateway_path = Arc::new(StdMutex::new(None));
        let dave_group_id = Arc::new(StdMutex::new(None));
        let discovery_count = Arc::new(Mutex::new(0usize));
        let speaking_observed = Arc::new(Notify::new());
        let audio_frame_count = Arc::new(Mutex::new(0usize));
        let audio_frame_times = Arc::new(Mutex::new(Vec::new()));
        let heartbeat_count = Arc::new(Mutex::new(0usize));
        let saw_identify = Arc::new(Mutex::new(false));
        let saw_resume = Arc::new(Mutex::new(false));
        let saw_select_protocol = Arc::new(Mutex::new(false));
        let session_description_sent = Arc::new(Mutex::new(false));
        let saw_dave_transition = Arc::new(Mutex::new(false));
        let saw_unmatched_dave_transition = Arc::new(Mutex::new(false));
        let saw_dave_prepare_epoch = Arc::new(Mutex::new(false));
        let saw_dave_key_package_before_prepare_epoch = Arc::new(Mutex::new(false));
        let saw_dave_key_package_after_prepare_epoch = Arc::new(Mutex::new(false));
        let saw_dave_key_package_before_external_sender = Arc::new(Mutex::new(false));
        let saw_dave_key_package_after_external_sender = Arc::new(Mutex::new(false));
        let saw_dave_commit_welcome = Arc::new(Mutex::new(false));
        let saw_dave_init_transition_ready = Arc::new(Mutex::new(false));
        let saw_dave_init_transition_ready_before_prepare_commit_transition =
            Arc::new(Mutex::new(false));
        let sent_dave_prepare_commit_transition = Arc::new(Mutex::new(false));
        let identified_user_id = Arc::new(Mutex::new(None::<String>));

        let discovery_count_state = Arc::clone(&discovery_count);
        let audio_frame_count_state = Arc::clone(&audio_frame_count);
        let audio_frame_times_state = Arc::clone(&audio_frame_times);
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let Ok((len, from)) = udp_socket.recv_from(&mut buf).await else {
                    break;
                };

                if len == 74 && buf[..2] == 1u16.to_be_bytes() {
                    *discovery_count_state.lock().await += 1;

                    let mut response = [0u8; 74];
                    response[..2].copy_from_slice(&2u16.to_be_bytes());
                    response[2..4].copy_from_slice(&70u16.to_be_bytes());
                    response[4..8].copy_from_slice(&buf[4..8]);
                    response[8..17].copy_from_slice(b"127.0.0.1");
                    response[72..74].copy_from_slice(&from.port().to_be_bytes());
                    udp_socket.send_to(&response, from).await.unwrap();
                    continue;
                }

                if len >= 12 {
                    *audio_frame_count_state.lock().await += 1;
                    audio_frame_times_state.lock().await.push(Instant::now());
                }
            }
        });

        let gateway_path_state = Arc::clone(&gateway_path);
        let speaking_observed_state = Arc::clone(&speaking_observed);
        let heartbeat_count_state = Arc::clone(&heartbeat_count);
        let saw_identify_state = Arc::clone(&saw_identify);
        let saw_resume_state = Arc::clone(&saw_resume);
        let saw_select_protocol_state = Arc::clone(&saw_select_protocol);
        let session_description_state = Arc::clone(&session_description_sent);
        let saw_dave_transition_state = Arc::clone(&saw_dave_transition);
        let saw_unmatched_dave_transition_state = Arc::clone(&saw_unmatched_dave_transition);
        let saw_dave_prepare_epoch_state = Arc::clone(&saw_dave_prepare_epoch);
        let saw_dave_key_package_before_prepare_epoch_state =
            Arc::clone(&saw_dave_key_package_before_prepare_epoch);
        let saw_dave_key_package_after_prepare_epoch_state =
            Arc::clone(&saw_dave_key_package_after_prepare_epoch);
        let saw_dave_key_package_before_external_sender_state =
            Arc::clone(&saw_dave_key_package_before_external_sender);
        let saw_dave_key_package_after_external_sender_state =
            Arc::clone(&saw_dave_key_package_after_external_sender);
        let saw_dave_commit_welcome_state = Arc::clone(&saw_dave_commit_welcome);
        let saw_dave_init_transition_ready_state = Arc::clone(&saw_dave_init_transition_ready);
        let saw_dave_init_transition_ready_before_prepare_commit_transition_state =
            Arc::clone(&saw_dave_init_transition_ready_before_prepare_commit_transition);
        let sent_dave_prepare_commit_transition_state =
            Arc::clone(&sent_dave_prepare_commit_transition);
        let identified_user_id_state = Arc::clone(&identified_user_id);
        let dave_group_id_state = Arc::clone(&dave_group_id);
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            sleep(gateway_delay).await;
            let mut dave_external_sender = None::<DaveExternalSender>;
            let mut dave_creator = None::<DaveSession>;
            let mut queued_stray_welcome = None::<Vec<u8>>;
            let mut queued_init_prepare_commit_transition = None::<Vec<u8>>;
            let mut sent_initial_external_sender = false;
            let mut dave_key_package_count = 0usize;
            let mut delayed_prepare_epoch_after_proposals =
                matches!(
                    dave_scenario,
                    DaveScenario::NewGroup
                        | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                        | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                );
            let mut delayed_prepare_epoch_after_commit =
                dave_scenario == DaveScenario::NewGroupCommitBeforePrepareEpoch;
            let mut ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
                *gateway_path_state.lock().unwrap() = Some(request.uri().to_string());
                Ok(response)
            })
            .await
            .unwrap();

            ws.send(Message::Text(
                json!({
                    "op": 8,
                    "d": { "heartbeat_interval": heartbeat_interval_ms }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

            while let Some(message) = ws.next().await {
                let Ok(message) = message else {
                    break;
                };
                if let Message::Text(text) = message {
                    let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                    match payload.get("op").and_then(Value::as_u64) {
                        Some(0) => {
                            let identify = payload.get("d").cloned().unwrap_or(Value::Null);
                            let user_id = identify
                                .get("user_id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned();
                            let required_fields_present = identify
                                .get("server_id")
                                .and_then(Value::as_str)
                                .is_some_and(|value| !value.is_empty())
                                && !user_id.is_empty()
                                && identify
                                    .get("session_id")
                                    .and_then(Value::as_str)
                                    .is_some_and(|value| !value.is_empty())
                                && identify
                                    .get("token")
                                    .and_then(Value::as_str)
                                    .is_some_and(|value| !value.is_empty());
                            *saw_identify_state.lock().await = required_fields_present;
                            *identified_user_id_state.lock().await =
                                required_fields_present.then_some(user_id.clone());
                            if !required_fields_present {
                                continue;
                            }
                            sleep(ready_delay).await;
                            ws.send(Message::Text(
                                json!({
                                    "op": 2,
                                    "d": {
                                        "ssrc": 7,
                                        "ip": udp_addr.ip().to_string(),
                                        "port": udp_addr.port(),
                                        "modes": [PREFERRED_MODE, REQUIRED_MODE],
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        }
                        Some(7) => {
                            *saw_resume_state.lock().await = true;
                            ws.send(Message::Text(
                                json!({
                                    "op": 9,
                                    "d": {}
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        }
                        Some(1) => {
                            *saw_select_protocol_state.lock().await = true;
                            *session_description_state.lock().await = true;
                            let mode = payload
                                .pointer("/d/data/mode")
                                .and_then(Value::as_str)
                                .unwrap_or(PREFERRED_MODE);
                            for event in &pre_session_description_events {
                                let payload = match event {
                                    PreSessionDescriptionEvent::ClientsConnect(user_ids) => json!({
                                        "op": 11,
                                        "seq": 1,
                                        "d": { "user_ids": user_ids }
                                    }),
                                    PreSessionDescriptionEvent::ClientDisconnect(user_id) => {
                                        json!({
                                            "op": 13,
                                            "seq": 2,
                                            "d": { "user_id": user_id }
                                        })
                                    }
                                    PreSessionDescriptionEvent::Speaking { user_id, ssrc } => {
                                        json!({
                                            "op": 5,
                                            "seq": 3,
                                            "d": {
                                                "speaking": 1,
                                                "delay": 0,
                                                "ssrc": ssrc,
                                                "user_id": user_id,
                                            }
                                        })
                                    }
                                    PreSessionDescriptionEvent::HeartbeatAck(nonce) => json!({
                                        "op": 6,
                                        "seq": 4,
                                        "d": nonce
                                    }),
                                };

                                ws.send(Message::Text(payload.to_string().into()))
                                    .await
                                    .unwrap();
                            }
                            ws.send(Message::Text(
                                json!({
                                    "op": 4,
                                    "d": {
                                        "mode": mode,
                                        "secret_key": vec![0u8; 32],
                                        "dave_protocol_version": (dave_scenario
                                            != DaveScenario::Disabled)
                                            .then_some(DAVE_PROTOCOL_VERSION),
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                            if dave_scenario != DaveScenario::Disabled {
                                let announced_user_ids = match dave_scenario {
                                    DaveScenario::Disabled => Vec::new(),
                                    DaveScenario::NewGroupSelfOnlyNoProposals => Vec::new(),
                                    DaveScenario::NewGroup
                                    | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                    | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                    | DaveScenario::NewGroupCommitBeforePrepareEpoch
                                    | DaveScenario::NewGroupRequiresInitKeyPackage
                                    | DaveScenario::NewGroupRequiresRefreshedKeyPackage
                                    | DaveScenario::PrepareBackedWelcomeWithStrayFollowUp
                                    | DaveScenario::UnmatchedWelcome => {
                                        vec![DAVE_CREATOR_USER_ID]
                                    }
                                    DaveScenario::EstablishedGroupJoin => {
                                        vec![DAVE_CREATOR_USER_ID, DAVE_EXISTING_MEMBER_USER_ID]
                                    }
                                };
                                ws.send(Message::Text(
                                    json!({
                                        "op": 11,
                                        "seq": 1,
                                        "d": {
                                            "user_ids": announced_user_ids,
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                                let group_id = dave_group_id_state.lock().unwrap().unwrap_or(2);
                                let external_sender =
                                    DaveExternalSender::new(group_id).expect("external sender");
                                let external_sender_bytes = external_sender
                                    .marshalled_external_sender()
                                    .expect("external sender bytes");
                                let mut creator = DaveSession::new(None).expect("creator session");
                                creator
                                    .set_external_sender(&external_sender_bytes)
                                    .expect("creator external sender");
                                creator
                                    .init(DAVE_PROTOCOL_VERSION, group_id, DAVE_CREATOR_USER_ID)
                                    .expect("creator init");
                                if dave_scenario == DaveScenario::EstablishedGroupJoin {
                                    let mut existing_member =
                                        DaveSession::new(None).expect("existing member session");
                                    existing_member
                                        .set_external_sender(&external_sender_bytes)
                                        .expect("existing external sender");
                                    existing_member
                                        .init(
                                            DAVE_PROTOCOL_VERSION,
                                            group_id,
                                            DAVE_EXISTING_MEMBER_USER_ID,
                                        )
                                        .expect("existing member init");
                                    let existing_key_package = existing_member
                                        .key_package()
                                        .expect("existing member key package");
                                    let proposal = external_sender
                                        .propose_add(0, &existing_key_package)
                                        .expect("existing member add proposal");
                                    let recognized_user_ids =
                                        [DAVE_CREATOR_USER_ID, DAVE_EXISTING_MEMBER_USER_ID];
                                    let commit_welcome = creator
                                        .process_proposals(&proposal, &recognized_user_ids)
                                        .expect("creator process proposals");
                                    let (commit, welcome) = external_sender
                                        .split_commit_welcome(&commit_welcome)
                                        .expect("split existing member commit/welcome");
                                    creator
                                        .process_commit(&commit)
                                        .expect("creator process existing member commit");
                                    existing_member
                                        .process_welcome(&welcome, &recognized_user_ids)
                                        .expect("existing member welcome");
                                }
                                if !matches!(
                                    dave_scenario,
                                    DaveScenario::NewGroupCommitBeforePrepareEpoch
                                        | DaveScenario::NewGroupRequiresInitKeyPackage
                                        | DaveScenario::NewGroupRequiresRefreshedKeyPackage
                                ) {
                                    ws.send(Message::Binary(Bytes::from(
                                        dave_external_sender_message(2, &external_sender_bytes),
                                    )))
                                    .await
                                    .unwrap();
                                    sent_initial_external_sender = true;
                                }
                                if dave_scenario != DaveScenario::EstablishedGroupJoin
                                    && !matches!(
                                        dave_scenario,
                                        DaveScenario::NewGroupCommitBeforePrepareEpoch
                                            | DaveScenario::NewGroupRequiresInitKeyPackage
                                            | DaveScenario::NewGroupRequiresRefreshedKeyPackage
                                    )
                                    && !matches!(
                                        dave_scenario,
                                        DaveScenario::NewGroup
                                            | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                            | DaveScenario::NewGroupSelfOnlyNoProposals
                                            | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                    )
                                {
                                    let transition_id = if matches!(
                                        dave_scenario,
                                        DaveScenario::NewGroup
                                            | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                            | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                    ) {
                                        DAVE_INIT_TRANSITION_ID
                                    } else {
                                        DAVE_TRANSITION_ID
                                    };
                                    ws.send(Message::Text(
                                        json!({
                                            "op": 24,
                                            "seq": 3,
                                            "d": {
                                                "transition_id": transition_id,
                                                "epoch": "1",
                                                "protocol_version": DAVE_PROTOCOL_VERSION,
                                            }
                                        })
                                        .to_string()
                                        .into(),
                                    ))
                                    .await
                                    .unwrap();
                                    *saw_dave_prepare_epoch_state.lock().await = true;
                                }
                                dave_external_sender = Some(external_sender);
                                dave_creator = Some(creator);
                            }
                        }
                        Some(23) if dave_scenario != DaveScenario::Disabled => {
                            let Some(transition_id) = payload
                                .pointer("/d/transition_id")
                                .and_then(Value::as_u64)
                                .and_then(|value| u16::try_from(value).ok())
                            else {
                                continue;
                            };

                            if transition_id == DAVE_TRANSITION_ID {
                                *saw_dave_transition_state.lock().await = true;
                                if let Some(stray_welcome) = queued_stray_welcome.take() {
                                    ws.send(Message::Binary(Bytes::from(stray_welcome)))
                                        .await
                                        .unwrap();
                                }
                                ws.send(Message::Text(
                                    json!({
                                        "op": 22,
                                        "seq": 3,
                                        "d": {
                                            "transition_id": DAVE_TRANSITION_ID,
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                            } else if transition_id == DAVE_INIT_TRANSITION_ID {
                                *saw_dave_init_transition_ready_state.lock().await = true;
                                if queued_init_prepare_commit_transition.is_some()
                                    && !*sent_dave_prepare_commit_transition_state.lock().await
                                {
                                    *saw_dave_init_transition_ready_before_prepare_commit_transition_state
                                        .lock()
                                        .await = true;
                                }
                                if let Some(prepare_commit_message) =
                                    queued_init_prepare_commit_transition.take()
                                {
                                    *sent_dave_prepare_commit_transition_state.lock().await = true;
                                    ws.send(Message::Binary(Bytes::from(prepare_commit_message)))
                                        .await
                                        .unwrap();
                                }
                            } else if transition_id == DAVE_UNMATCHED_TRANSITION_ID {
                                *saw_unmatched_dave_transition_state.lock().await = true;
                                ws.send(Message::Text(
                                    json!({
                                        "op": 22,
                                        "seq": 3,
                                        "d": {
                                            "transition_id": DAVE_UNMATCHED_TRANSITION_ID,
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                            }
                        }
                        Some(5) => speaking_observed_state.notify_one(),
                        Some(3) => *heartbeat_count_state.lock().await += 1,
                        _ => {}
                    }
                } else if let Message::Binary(bytes) = message
                    && dave_scenario != DaveScenario::Disabled
                {
                    match bytes.first().copied() {
                        Some(26) => {
                            dave_key_package_count += 1;
                            if !sent_initial_external_sender
                                && matches!(
                                    dave_scenario,
                                    DaveScenario::NewGroupCommitBeforePrepareEpoch
                                        | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                        | DaveScenario::NewGroupRequiresInitKeyPackage
                                        | DaveScenario::NewGroupRequiresRefreshedKeyPackage
                                )
                            {
                                *saw_dave_key_package_before_external_sender_state
                                    .lock()
                                    .await = true;
                                let external_sender = dave_external_sender
                                    .as_ref()
                                    .expect("external sender missing");
                                let external_sender_bytes = external_sender
                                    .marshalled_external_sender()
                                    .expect("external sender bytes");
                                ws.send(Message::Binary(Bytes::from(
                                    dave_external_sender_message(2, &external_sender_bytes),
                                )))
                                .await
                                .unwrap();
                                sent_initial_external_sender = true;
                            }
                            if dave_key_package_count == 1 {
                                continue;
                            }
                            if sent_initial_external_sender
                                && matches!(
                                    dave_scenario,
                                    DaveScenario::NewGroupCommitBeforePrepareEpoch
                                        | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                        | DaveScenario::NewGroupRequiresInitKeyPackage
                                        | DaveScenario::NewGroupRequiresRefreshedKeyPackage
                                )
                                && !*saw_dave_prepare_epoch_state.lock().await
                            {
                                *saw_dave_key_package_after_external_sender_state
                                    .lock()
                                    .await = true;
                                ws.send(Message::Text(
                                    json!({
                                        "op": 24,
                                        "seq": 3,
                                        "d": {
                                            "transition_id": DAVE_INIT_TRANSITION_ID,
                                            "epoch": "1",
                                            "protocol_version": DAVE_PROTOCOL_VERSION,
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                                *saw_dave_prepare_epoch_state.lock().await = true;
                            } else if sent_initial_external_sender
                                && matches!(
                                    dave_scenario,
                                    DaveScenario::NewGroup
                                        | DaveScenario::NewGroupSelfOnlyNoProposals
                                        | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                        | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                        | DaveScenario::EstablishedGroupJoin
                                        | DaveScenario::PrepareBackedWelcomeWithStrayFollowUp
                                        | DaveScenario::UnmatchedWelcome
                                )
                            {
                                *saw_dave_key_package_after_external_sender_state
                                    .lock()
                                    .await = true;
                            }
                            if *saw_dave_prepare_epoch_state.lock().await {
                                *saw_dave_key_package_after_prepare_epoch_state.lock().await = true;
                            } else {
                                *saw_dave_key_package_before_prepare_epoch_state.lock().await =
                                    true;
                            }
                            let user_id = identified_user_id_state
                                .lock()
                                .await
                                .clone()
                                .unwrap_or_else(|| "1111111111111111".to_owned());
                            let external_sender = dave_external_sender
                                .as_ref()
                                .expect("external sender missing");
                            let creator = dave_creator.as_mut().expect("creator session missing");
                            if dave_scenario == DaveScenario::PrepareBackedWelcomeWithStrayFollowUp {
                                let mut stray_creator =
                                    DaveSession::new(None).expect("stray creator session");
                                let external_sender_bytes = external_sender
                                    .marshalled_external_sender()
                                    .expect("stray external sender bytes");
                                stray_creator
                                    .set_external_sender(&external_sender_bytes)
                                    .expect("stray creator external sender");
                                stray_creator
                                    .init(
                                        DAVE_PROTOCOL_VERSION,
                                        group_id_from_user_context(&user_id, &dave_group_id_state),
                                        DAVE_CREATOR_USER_ID,
                                    )
                                    .expect("stray creator init");
                                queued_stray_welcome = Some(dave_welcome_message(
                                    5,
                                    &mut stray_creator,
                                    external_sender,
                                    DaveScenario::NewGroup,
                                    &user_id,
                                    DAVE_UNMATCHED_TRANSITION_ID,
                                    &bytes[1..],
                                ));
                            }

                            let transition_id = match dave_scenario {
                                DaveScenario::NewGroup
                                | DaveScenario::NewGroupSelfOnlyNoProposals
                                | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                | DaveScenario::NewGroupCommitBeforePrepareEpoch
                                | DaveScenario::NewGroupRequiresInitKeyPackage
                                | DaveScenario::NewGroupRequiresRefreshedKeyPackage => {
                                    DAVE_INIT_TRANSITION_ID
                                }
                                DaveScenario::UnmatchedWelcome => DAVE_UNMATCHED_TRANSITION_ID,
                                _ => DAVE_TRANSITION_ID,
                            };

                            if dave_scenario == DaveScenario::NewGroupSelfOnlyNoProposals {
                                continue;
                            } else if matches!(
                                dave_scenario,
                                DaveScenario::NewGroup
                                    | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                    | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                    | DaveScenario::NewGroupCommitBeforePrepareEpoch
                                    | DaveScenario::NewGroupRequiresInitKeyPackage
                                    | DaveScenario::NewGroupRequiresRefreshedKeyPackage
                            ) {
                                let proposal_message = {
                                    let creator_key_package =
                                        creator.key_package().expect("creator key package");
                                    let proposal = external_sender
                                        .propose_add(0, &creator_key_package)
                                        .expect("runtime proposal");
                                    dave_proposals_message(4, &proposal)
                                };
                                if dave_scenario
                                    == DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                {
                                    ws.send(Message::Binary(Bytes::from(dave_revoke_message(
                                        4,
                                        &[0],
                                    ))))
                                    .await
                                    .unwrap();
                                }
                                ws.send(Message::Binary(Bytes::from(proposal_message)))
                                    .await
                                    .unwrap();
                                if delayed_prepare_epoch_after_proposals {
                                    ws.send(Message::Text(
                                        json!({
                                            "op": 24,
                                            "seq": 3,
                                            "d": {
                                                "transition_id": DAVE_INIT_TRANSITION_ID,
                                                "epoch": "1",
                                                "protocol_version": DAVE_PROTOCOL_VERSION,
                                            }
                                        })
                                        .to_string()
                                        .into(),
                                    ))
                                    .await
                                    .unwrap();
                                    *saw_dave_prepare_epoch_state.lock().await = true;
                                    delayed_prepare_epoch_after_proposals = false;
                                }
                            } else {
                                let welcome_message = dave_welcome_message(
                                    4,
                                    creator,
                                    external_sender,
                                    dave_scenario,
                                    &user_id,
                                    transition_id,
                                    &bytes[1..],
                                );
                                ws.send(Message::Binary(Bytes::from(welcome_message)))
                                    .await
                                    .unwrap();
                            }
                        }
                        Some(28)
                            if matches!(
                                dave_scenario,
                                DaveScenario::NewGroupSelfOnlyNoProposals
                            ) =>
                        {
                            *saw_dave_commit_welcome_state.lock().await = true;
                        }
                        Some(28)
                            if matches!(
                                dave_scenario,
                                DaveScenario::NewGroup
                                    | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
                                    | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                                    | DaveScenario::NewGroupCommitBeforePrepareEpoch
                                    | DaveScenario::NewGroupRequiresInitKeyPackage
                                    | DaveScenario::NewGroupRequiresRefreshedKeyPackage
                            ) =>
                        {
                            *saw_dave_commit_welcome_state.lock().await = true;
                            let user_id = identified_user_id_state
                                .lock()
                                .await
                                .clone()
                                .unwrap_or_else(|| "1111111111111111".to_owned());
                            let prepare_commit_message = {
                                let creator =
                                    dave_creator.as_mut().expect("creator session missing");
                                let recognized_user_ids = [DAVE_CREATOR_USER_ID, user_id.as_str()];
                                let (commit, welcome) =
                                    split_dave_mls_commit_welcome_payload(&bytes[1..])
                                        .expect("split raw commit/welcome");
                                let welcome = welcome.expect("commit/welcome includes welcome");
                                creator
                                    .process_welcome(&welcome, &recognized_user_ids)
                                    .expect("creator process welcome");
                                dave_prepare_commit_transition_message(
                                    5,
                                    DAVE_INIT_TRANSITION_ID,
                                    &commit,
                                )
                            };
                            if dave_scenario
                                == DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
                            {
                                queued_init_prepare_commit_transition =
                                    Some(prepare_commit_message);
                            } else {
                                *sent_dave_prepare_commit_transition_state.lock().await = true;
                                ws.send(Message::Binary(Bytes::from(prepare_commit_message)))
                                    .await
                                    .unwrap();
                            }
                            if delayed_prepare_epoch_after_commit {
                                ws.send(Message::Text(
                                    json!({
                                        "op": 24,
                                        "seq": 3,
                                        "d": {
                                            "transition_id": DAVE_INIT_TRANSITION_ID,
                                            "epoch": "1",
                                            "protocol_version": DAVE_PROTOCOL_VERSION,
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                                *saw_dave_prepare_epoch_state.lock().await = true;
                                delayed_prepare_epoch_after_commit = false;
                            }
                        }
                        _ => {}
                    }
                }
            }
        });

        Self {
            endpoint_host: ws_addr.to_string(),
            gateway_path,
            dave_group_id,
            discovery_count,
            speaking_observed,
            audio_frame_count,
            audio_frame_times,
            heartbeat_count,
            saw_identify,
            saw_resume,
            saw_select_protocol,
            session_description_sent,
            saw_dave_transition,
            saw_unmatched_dave_transition,
            saw_dave_prepare_epoch,
            saw_dave_key_package_before_prepare_epoch,
            saw_dave_key_package_after_prepare_epoch,
            saw_dave_key_package_before_external_sender,
            saw_dave_key_package_after_external_sender,
            saw_dave_commit_welcome,
            saw_dave_init_transition_ready,
            saw_dave_init_transition_ready_before_prepare_commit_transition,
            sent_dave_prepare_commit_transition,
        }
    }

    pub fn endpoint(&self) -> String {
        self.endpoint_host.clone()
    }

    pub fn voice_context(
        &self,
        guild_id: &str,
        channel_id: &str,
        user_id: &str,
        session_id: &str,
        token: &str,
    ) -> VoiceContext {
        *self.dave_group_id.lock().unwrap() = channel_id.parse::<u64>().ok();
        VoiceContext {
            guild_id: guild_id.into(),
            channel_id: channel_id.into(),
            user_id: user_id.into(),
            session_id: session_id.into(),
            endpoint: self.endpoint(),
            token: token.into(),
        }
    }

    pub async fn gateway_path(&self) -> Option<String> {
        wait_for_sync_value(&self.gateway_path).await
    }

    pub async fn discovery_count(&self) -> usize {
        wait_for_value(&self.discovery_count, |count| *count >= 1).await
    }

    pub fn speaking_observed(&self) -> Arc<Notify> {
        Arc::clone(&self.speaking_observed)
    }

    pub async fn audio_frame_count_at_least(&self, minimum: usize) -> usize {
        wait_for_value(&self.audio_frame_count, |count| *count >= minimum).await
    }

    pub async fn audio_frame_span_for_first(&self, frame_count: usize) -> Duration {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let timestamps = self.audio_frame_times.lock().await.clone();
            if timestamps.len() >= frame_count {
                return timestamps[frame_count - 1].saturating_duration_since(timestamps[0]);
            }
            if Instant::now() >= deadline {
                return Duration::ZERO;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn heartbeat_count_at_least(&self, minimum: usize) -> usize {
        wait_for_value(&self.heartbeat_count, |count| *count >= minimum).await
    }

    pub async fn saw_identify(&self) -> bool {
        wait_for_value(&self.saw_identify, |ready| *ready).await
    }

    pub async fn saw_resume(&self) -> bool {
        wait_for_value(&self.saw_resume, |ready| *ready).await
    }

    pub async fn saw_select_protocol(&self) -> bool {
        wait_for_value(&self.saw_select_protocol, |ready| *ready).await
    }

    pub async fn session_description_sent(&self) -> bool {
        wait_for_value(&self.session_description_sent, |ready| *ready).await
    }

    pub async fn saw_dave_transition(&self) -> bool {
        wait_for_value(&self.saw_dave_transition, |ready| *ready).await
    }

    pub async fn saw_dave_transition_within(&self, timeout: Duration) -> bool {
        wait_for_value_with_timeout(&self.saw_dave_transition, timeout, |ready| *ready).await
    }

    pub async fn saw_unmatched_dave_transition(&self) -> bool {
        *self.saw_unmatched_dave_transition.lock().await
    }

    pub async fn saw_dave_prepare_epoch(&self) -> bool {
        wait_for_value(&self.saw_dave_prepare_epoch, |ready| *ready).await
    }

    pub async fn saw_dave_prepare_epoch_within(&self, timeout: Duration) -> bool {
        wait_for_value_with_timeout(&self.saw_dave_prepare_epoch, timeout, |ready| *ready).await
    }

    pub async fn saw_dave_key_package_before_prepare_epoch(&self) -> bool {
        wait_for_value(&self.saw_dave_key_package_before_prepare_epoch, |ready| {
            *ready
        })
        .await
    }

    pub async fn saw_dave_key_package_after_prepare_epoch(&self) -> bool {
        wait_for_value(&self.saw_dave_key_package_after_prepare_epoch, |ready| {
            *ready
        })
        .await
    }

    pub async fn saw_dave_key_package_before_external_sender(&self) -> bool {
        wait_for_value(&self.saw_dave_key_package_before_external_sender, |ready| {
            *ready
        })
        .await
    }

    pub async fn saw_dave_key_package_after_external_sender(&self) -> bool {
        wait_for_value(&self.saw_dave_key_package_after_external_sender, |ready| {
            *ready
        })
        .await
    }

    pub async fn saw_dave_commit_welcome(&self) -> bool {
        wait_for_value(&self.saw_dave_commit_welcome, |ready| *ready).await
    }

    pub async fn saw_dave_init_transition_ready(&self) -> bool {
        wait_for_value(&self.saw_dave_init_transition_ready, |ready| *ready).await
    }

    pub async fn saw_dave_init_transition_ready_before_prepare_commit_transition(&self) -> bool {
        wait_for_value(
            &self.saw_dave_init_transition_ready_before_prepare_commit_transition,
            |ready| *ready,
        )
        .await
    }

    pub async fn sent_dave_prepare_commit_transition(&self) -> bool {
        wait_for_value(&self.sent_dave_prepare_commit_transition, |ready| *ready).await
    }

    pub async fn sent_dave_prepare_commit_transition_within(&self, timeout: Duration) -> bool {
        wait_for_value_with_timeout(
            &self.sent_dave_prepare_commit_transition,
            timeout,
            |ready| *ready,
        )
        .await
    }
}

fn group_id_from_user_context(
    _runtime_user_id: &str,
    dave_group_id_state: &Arc<StdMutex<Option<u64>>>,
) -> u64 {
    dave_group_id_state.lock().unwrap().unwrap_or(2)
}

fn dave_external_sender_message(sequence: u16, external_sender_bytes: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(3 + external_sender_bytes.len());
    message.extend_from_slice(&sequence.to_be_bytes());
    message.push(25);
    message.extend_from_slice(external_sender_bytes);
    message
}

fn dave_proposals_message(sequence: u16, proposals: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(4 + proposals.len());
    message.extend_from_slice(&sequence.to_be_bytes());
    message.push(27);
    message.push(0);
    message.extend_from_slice(proposals);
    message
}

fn dave_revoke_message(sequence: u16, refs: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(4 + refs.len());
    message.extend_from_slice(&sequence.to_be_bytes());
    message.push(27);
    message.push(1);
    message.extend_from_slice(refs);
    message
}

fn dave_prepare_commit_transition_message(
    sequence: u16,
    transition_id: u16,
    commit: &[u8],
) -> Vec<u8> {
    let mut message = Vec::with_capacity(5 + commit.len());
    message.extend_from_slice(&sequence.to_be_bytes());
    message.push(29);
    message.extend_from_slice(&transition_id.to_be_bytes());
    message.extend_from_slice(commit);
    message
}

fn dave_welcome_message(
    sequence: u16,
    creator: &mut DaveSession,
    external_sender: &DaveExternalSender,
    dave_scenario: DaveScenario,
    runtime_user_id: &str,
    transition_id: u16,
    key_package: &[u8],
) -> Vec<u8> {
    let proposal_epoch = match dave_scenario {
        DaveScenario::Disabled => unreachable!("disabled DAVE scenario cannot emit welcome"),
        DaveScenario::NewGroup
        | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
        | DaveScenario::NewGroupSelfOnlyNoProposals
        | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
        | DaveScenario::NewGroupCommitBeforePrepareEpoch
        | DaveScenario::NewGroupRequiresInitKeyPackage
        | DaveScenario::NewGroupRequiresRefreshedKeyPackage
        | DaveScenario::PrepareBackedWelcomeWithStrayFollowUp
        | DaveScenario::UnmatchedWelcome => 0,
        DaveScenario::EstablishedGroupJoin => 1,
    };
    let proposal = external_sender
        .propose_add(proposal_epoch, key_package)
        .expect("runtime proposal");
    let recognized_user_ids = match dave_scenario {
        DaveScenario::Disabled => unreachable!("disabled DAVE scenario cannot emit welcome"),
        DaveScenario::NewGroup
        | DaveScenario::NewGroupWithNoOpRevokeBeforeProposals
        | DaveScenario::NewGroupSelfOnlyNoProposals
        | DaveScenario::NewGroupRequiresInitTransitionReadyBeforePrepareCommitTransition
        | DaveScenario::NewGroupCommitBeforePrepareEpoch
        | DaveScenario::NewGroupRequiresInitKeyPackage
        | DaveScenario::NewGroupRequiresRefreshedKeyPackage
        | DaveScenario::PrepareBackedWelcomeWithStrayFollowUp
        | DaveScenario::UnmatchedWelcome => {
            vec![DAVE_CREATOR_USER_ID, runtime_user_id]
        }
        DaveScenario::EstablishedGroupJoin => {
            vec![
                DAVE_CREATOR_USER_ID,
                DAVE_EXISTING_MEMBER_USER_ID,
                runtime_user_id,
            ]
        }
    };
    let commit_welcome = creator
        .process_proposals(&proposal, &recognized_user_ids)
        .expect("creator process proposals");
    let (commit, welcome) = external_sender
        .split_commit_welcome(&commit_welcome)
        .expect("split commit/welcome");
    creator
        .process_commit(&commit)
        .expect("creator process commit");

    let mut message = Vec::with_capacity(5 + welcome.len());
    message.extend_from_slice(&sequence.to_be_bytes());
    message.push(30);
    message.extend_from_slice(&transition_id.to_be_bytes());
    message.extend_from_slice(&welcome);
    message
}

async fn wait_for_value<T, F>(slot: &Arc<Mutex<T>>, ready: F) -> T
where
    T: Clone,
    F: Fn(&T) -> bool,
{
    wait_for_value_with_timeout(slot, Duration::from_secs(2), ready).await
}

async fn wait_for_value_with_timeout<T, F>(slot: &Arc<Mutex<T>>, timeout: Duration, ready: F) -> T
where
    T: Clone,
    F: Fn(&T) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let value = slot.lock().await.clone();
        if ready(&value) || Instant::now() >= deadline {
            return value;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_sync_value<T: Clone>(slot: &Arc<StdMutex<Option<T>>>) -> Option<T> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let value = slot.lock().unwrap().clone();
        if value.is_some() || Instant::now() >= deadline {
            return value;
        }
        sleep(Duration::from_millis(10)).await;
    }
}
