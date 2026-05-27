use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};

use davey::ProposalsOperationType;
use tokio::net::lookup_host;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep, timeout};

use crate::dave::{DaveExternalSender, DaveRuntimeContext, DaveSession};
use crate::error::VoiceError;
use crate::gateway::VoiceGatewayClient;
use crate::protocol::{
    self, ClientsConnect, DaveExecuteTransition, DaveMlsExternalSenderPackage,
    DaveMlsPrepareCommitTransition, DaveMlsProposals, DaveMlsWelcome, DavePrepareEpoch, Hello,
    Ready, SessionDescription, VoiceGatewayEvent,
};
use crate::session::VoiceContext;
use crate::udp::{DiscoveredUdpAddress, VoiceUdpTransport};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const POST_HELLO_TIMEOUT_FLOOR: Duration = Duration::from_secs(30);
const DAVE_PROTOCOL_INIT_TRANSITION_ID: u16 = 0;

pub struct VoiceHandshakeResult {
    pub gateway: VoiceGatewayClient,
    pub transport: VoiceUdpTransport,
    pub ssrc: u32,
    pub heartbeat_shutdown: oneshot::Sender<()>,
    pub session_description: SessionDescription,
    pub dave: Option<InitialDaveState>,
}

pub struct InitialDaveState {
    pub runtime: DaveRuntimeContext,
    pub group_id: u64,
    pub external_sender: DaveExternalSender,
    pub recognized_user_ids: BTreeSet<String>,
}

pub async fn connect(voice: &VoiceContext) -> Result<Option<VoiceHandshakeResult>, VoiceError> {
    let Some(gateway_url) = gateway_url(&voice.endpoint)? else {
        return Ok(None);
    };

    let gateway = timeout(HANDSHAKE_TIMEOUT, VoiceGatewayClient::connect(&gateway_url))
        .await
        .map_err(|_| VoiceError::InvalidState("voice gateway connect timed out"))??;
    let hello = expect_hello(&gateway).await?;
    tracing::debug!(
        heartbeat_interval_ms = hello.heartbeat_interval_ms,
        "voice handshake received hello"
    );
    let post_hello_timeout = post_hello_timeout(hello.heartbeat_interval_ms);
    let heartbeat_shutdown = spawn_heartbeat_task(gateway.clone(), hello.heartbeat_interval_ms);

    let connect_result = async {
        gateway.send_identify(voice).await?;

        let ready = expect_ready(&gateway, post_hello_timeout).await?;
        let mode = protocol::choose_encryption_mode(&ready)?.to_owned();
        let udp_target = resolve_udp_target(&ready).await?;
        let transport = VoiceUdpTransport::connect(udp_target, ready.ssrc).await?;
        let discovered = DiscoveredUdpAddress {
            ip: transport.local_addr().ip().to_string(),
            port: transport.local_addr().port(),
        };

        gateway.send_select_protocol(&discovered, &mode).await?;
        let session_description =
            expect_session_description(&gateway, post_hello_timeout, &voice.user_id).await?;
        let dave = if let Some(protocol_version) = session_description.dave_protocol_version {
            Some(
                complete_initial_dave_transition(
                    &gateway,
                    voice,
                    ready.ssrc,
                    protocol_version,
                    post_hello_timeout,
                )
                .await?,
            )
        } else {
            None
        };

        Ok::<_, VoiceError>((transport, ready.ssrc, session_description, dave))
    }
    .await;

    let (transport, ssrc, session_description, dave) = match connect_result {
        Ok(result) => result,
        Err(err) => {
            let _ = heartbeat_shutdown.send(());
            return Err(err);
        }
    };

    Ok(Some(VoiceHandshakeResult {
        gateway,
        transport,
        ssrc,
        heartbeat_shutdown,
        session_description,
        dave,
    }))
}

pub async fn resume(voice: &VoiceContext, seq_ack: Option<u64>) -> Result<(), VoiceError> {
    let Some(gateway_url) = gateway_url(&voice.endpoint)? else {
        return Err(VoiceError::InvalidState(
            "voice endpoint invalid for resume",
        ));
    };

    let gateway = timeout(HANDSHAKE_TIMEOUT, VoiceGatewayClient::connect(&gateway_url))
        .await
        .map_err(|_| VoiceError::InvalidState("voice gateway connect timed out"))??;
    let hello = expect_hello(&gateway).await?;
    let post_hello_timeout = post_hello_timeout(hello.heartbeat_interval_ms);

    if let Some(seq_ack) = seq_ack {
        gateway.record_seq_ack(seq_ack).await;
    }
    gateway
        .send_resume(&voice.guild_id, &voice.session_id, &voice.token)
        .await?;

    match next_event(&gateway, post_hello_timeout).await?.into_event() {
        VoiceGatewayEvent::Resumed => Ok(()),
        _ => Err(VoiceError::InvalidState("voice handshake resume rejected")),
    }
}

fn gateway_url(endpoint: &str) -> Result<Option<String>, VoiceError> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if let Ok(uri) = trimmed.parse::<http::Uri>()
        && uri.scheme().is_some()
        && uri.authority().is_some()
    {
        return Ok(Some(normalize_absolute_gateway_url(&uri)?));
    }

    if looks_like_local_endpoint(trimmed)? {
        return Ok(Some(format!("ws://{trimmed}")));
    }

    if looks_like_forwarded_endpoint(trimmed)? {
        return Ok(Some(format!("wss://{trimmed}")));
    }

    Ok(None)
}

fn normalize_absolute_gateway_url(uri: &http::Uri) -> Result<String, VoiceError> {
    let mut parts = uri.clone().into_parts();
    let Some(scheme) = parts.scheme.as_ref().map(http::uri::Scheme::as_str) else {
        return Err(VoiceError::InvalidState("voice endpoint scheme missing"));
    };

    parts.scheme = Some(
        match scheme {
            "ws" => "ws",
            "wss" => "wss",
            "http" => "ws",
            "https" => "wss",
            _ => {
                return Err(VoiceError::InvalidState(
                    "voice endpoint scheme unsupported",
                ));
            }
        }
        .parse()
        .map_err(|_| VoiceError::InvalidState("voice endpoint scheme invalid"))?,
    );

    Ok(http::Uri::from_parts(parts)
        .map_err(|_| VoiceError::InvalidState("voice endpoint could not be normalized"))?
        .to_string())
}

fn looks_like_local_endpoint(endpoint: &str) -> Result<bool, VoiceError> {
    let Some(host) = endpoint_host(endpoint)? else {
        return Ok(false);
    };

    Ok(host == "localhost" || host.parse::<IpAddr>().map(is_local_ip).unwrap_or(false))
}

fn looks_like_forwarded_endpoint(endpoint: &str) -> Result<bool, VoiceError> {
    let Some(host) = endpoint_host(endpoint)? else {
        return Ok(false);
    };

    Ok(host.contains('.') && host != "localhost" && host.parse::<IpAddr>().is_err())
}

fn endpoint_host(endpoint: &str) -> Result<Option<String>, VoiceError> {
    let candidate = format!("https://{endpoint}");
    let uri = candidate
        .parse::<http::Uri>()
        .map_err(|_| VoiceError::InvalidState("voice endpoint invalid"))?;
    let Some(authority) = uri.authority() else {
        return Ok(None);
    };

    Ok(Some(authority.host().to_owned()))
}

fn is_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(addr) => addr.is_loopback() || addr.is_private(),
        IpAddr::V6(addr) => addr.is_loopback() || addr.is_unique_local(),
    }
}

async fn expect_hello(gateway: &VoiceGatewayClient) -> Result<Hello, VoiceError> {
    match next_event(gateway, HANDSHAKE_TIMEOUT).await?.into_event() {
        VoiceGatewayEvent::Hello(hello) => Ok(hello),
        _ => Err(VoiceError::InvalidState("voice handshake hello missing")),
    }
}

async fn expect_ready(
    gateway: &VoiceGatewayClient,
    timeout_duration: Duration,
) -> Result<Ready, VoiceError> {
    match next_event(gateway, timeout_duration).await?.into_event() {
        VoiceGatewayEvent::Ready(ready) => Ok(ready),
        _ => Err(VoiceError::InvalidState("voice handshake ready missing")),
    }
}

async fn expect_session_description(
    gateway: &VoiceGatewayClient,
    timeout_duration: Duration,
    self_user_id: &str,
) -> Result<SessionDescription, VoiceError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;

    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(VoiceError::InvalidState("voice handshake timed out"))?;

        match next_event(gateway, remaining).await?.into_event() {
            VoiceGatewayEvent::SessionDescription(description) => return Ok(description),
            event if is_benign_pre_session_description_event(&event, self_user_id) => {
                tracing::debug!(
                    event = dave_handshake_event_name(&event),
                    "voice handshake ignoring benign pre-session-description event"
                );
            }
            _ => {
                return Err(VoiceError::InvalidState(
                    "voice handshake session description missing",
                ));
            }
        }
    }
}

fn is_benign_pre_session_description_event(event: &VoiceGatewayEvent, self_user_id: &str) -> bool {
    matches!(
        event,
        VoiceGatewayEvent::ClientsConnect(_)
            | VoiceGatewayEvent::Speaking(_)
            | VoiceGatewayEvent::HeartbeatAck(_)
            | VoiceGatewayEvent::Video(_)
            | VoiceGatewayEvent::MediaSinkWants
            | VoiceGatewayEvent::ClientFlags(_)
            | VoiceGatewayEvent::ClientPlatform(_)
    ) || matches!(
        event,
        VoiceGatewayEvent::ClientDisconnect(disconnect) if disconnect.user_id != self_user_id
    )
}

async fn complete_initial_dave_transition(
    gateway: &VoiceGatewayClient,
    voice: &VoiceContext,
    ssrc: u32,
    protocol_version: u16,
    timeout_duration: Duration,
) -> Result<InitialDaveState, VoiceError> {
    let group_id = voice
        .channel_id
        .parse::<u64>()
        .map_err(|_| VoiceError::InvalidState("voice dave group id invalid"))?;
    let local_external_sender = DaveExternalSender::new(group_id)
        .map_err(|_| VoiceError::InvalidState("voice dave external sender create failed"))?;
    let mut recognized_user_ids = BTreeSet::from([voice.user_id.clone()]);
    let mut session = Some(
        DaveSession::new(None)
            .map_err(|_| VoiceError::InvalidState("voice dave session create failed"))?,
    );
    dave_session_mut(&mut session)?
        .init(protocol_version, group_id, &voice.user_id)
        .map_err(|_| VoiceError::InvalidState("voice dave session init failed"))?;
    tracing::debug!(
        group_id,
        protocol_version,
        user_id = %voice.user_id,
        ssrc,
        "voice dave handshake initialized session"
    );
    let mut pending_prepared_transitions = BTreeMap::<u16, u16>::new();
    let mut pending_transition = None::<(u16, DaveRuntimeContext)>;
    let mut pending_key_package = false;
    send_pending_join_key_package(
        gateway,
        dave_session_mut(&mut session)?,
        &mut pending_key_package,
    )
    .await?;
    let mut sent_key_package_before_external_sender = true;

    loop {
        let event = next_event(gateway, timeout_duration).await?.into_event();
        tracing::debug!(
            event = dave_handshake_event_name(&event),
            pending_key_package,
            pending_prepared_transitions = pending_prepared_transitions.len(),
            has_pending_transition = pending_transition.is_some(),
            recognized_user_ids = recognized_user_ids.len(),
            "voice dave handshake received gateway event"
        );

        if let VoiceGatewayEvent::DaveExecuteTransition(DaveExecuteTransition { transition_id }) =
            &event
        {
            tracing::debug!(
                transition_id,
                matched_pending_transition =
                    pending_transition
                        .as_ref()
                        .is_some_and(
                            |(expected_transition_id, _)| *transition_id == *expected_transition_id
                        ),
                "voice dave handshake processing execute transition"
            );
            if pending_transition
                .as_ref()
                .is_some_and(|(expected_transition_id, _)| {
                    *transition_id == *expected_transition_id
                })
            {
                let runtime = pending_transition.take().expect("pending transition").1;
                return Ok(initial_dave_state(
                    runtime,
                    group_id,
                    local_external_sender,
                    recognized_user_ids,
                ));
            }
            continue;
        }

        if pending_transition.is_some() {
            continue;
        }

        match event {
            VoiceGatewayEvent::ClientsConnect(ClientsConnect { user_ids }) => {
                let user_count = user_ids.len();
                recognized_user_ids.extend(user_ids);
                tracing::debug!(
                    user_count,
                    recognized_user_ids = recognized_user_ids.len(),
                    "voice dave handshake updated recognized users"
                );
            }
            VoiceGatewayEvent::DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage {
                external_sender: sender,
            }) => {
                tracing::debug!(
                    external_sender_len = sender.len(),
                    "voice dave handshake received external sender"
                );
                dave_session_mut(&mut session)?
                    .set_external_sender(&sender)
                    .map_err(|_| VoiceError::InvalidState("voice dave external sender invalid"))?;
                if sent_key_package_before_external_sender {
                    tracing::debug!(
                        "voice dave handshake refreshing key package after external sender"
                    );
                    pending_key_package = false;
                    send_pending_join_key_package(
                        gateway,
                        dave_session_mut(&mut session)?,
                        &mut pending_key_package,
                    )
                    .await?;
                    sent_key_package_before_external_sender = false;
                }
                if has_only_self_recognized_user(&recognized_user_ids, &voice.user_id) {
                    let recognized = [voice.user_id.as_str()];
                    tracing::debug!(
                        recognized_user_ids = recognized.len(),
                        "voice dave handshake creating self-only initial group without proposals"
                    );
                    let commit_welcome = dave_session_mut(&mut session)?
                        .process_proposals(&empty_mls_proposals(), &recognized)
                        .map_err(|_| {
                            VoiceError::InvalidState("voice dave self-only proposals invalid")
                        })?;
                    let (commit, _welcome) = local_external_sender
                        .split_commit_welcome(&commit_welcome)
                        .map_err(|_| {
                            VoiceError::InvalidState("voice dave commit welcome invalid")
                        })?;
                    let session = take_dave_session(&mut session)?;
                    let runtime = complete_local_initial_creator_transition(
                        gateway,
                        session,
                        &commit_welcome,
                        &commit,
                    )
                    .await?;
                    return Ok(initial_dave_state(
                        runtime,
                        group_id,
                        local_external_sender,
                        recognized_user_ids,
                    ));
                }
            }
            VoiceGatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
                transition_id,
                epoch,
                protocol_version: prepare_protocol_version,
            }) => {
                if prepare_protocol_version != protocol_version {
                    return Err(VoiceError::InvalidState(
                        "voice dave prepare epoch protocol version mismatch",
                    ));
                }
                if epoch.is_empty() {
                    return Err(VoiceError::InvalidState("voice dave prepare epoch missing"));
                }
                tracing::debug!(
                    transition_id,
                    epoch = %epoch,
                    prepare_protocol_version,
                    "voice dave handshake processing prepare epoch"
                );
                send_pending_join_key_package(
                    gateway,
                    dave_session_mut(&mut session)?,
                    &mut pending_key_package,
                )
                .await?;
                pending_prepared_transitions.insert(transition_id, prepare_protocol_version);
            }
            VoiceGatewayEvent::DaveMlsProposals(DaveMlsProposals {
                operation,
                proposals,
            }) => {
                if !pending_key_package {
                    return Err(VoiceError::InvalidState(
                        "voice dave proposals missing pending group creation",
                    ));
                }
                let recognized = recognized_user_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                tracing::debug!(
                    proposals_len = proposals.len(),
                    recognized_user_ids = recognized.len(),
                    "voice dave handshake processing proposals"
                );
                let commit_welcome = dave_session_mut(&mut session)?
                    .process_proposals_with_operation(operation, &proposals, &recognized)
                    .map_err(|_| VoiceError::InvalidState("voice dave proposals invalid"))?;
                let commit_welcome = match commit_welcome {
                    Some(commit_welcome) => commit_welcome,
                    None if ProposalsOperationType::from(operation)
                        == ProposalsOperationType::REVOKE =>
                    {
                        tracing::debug!(
                            "voice dave handshake proposal result did not require commit"
                        );
                        continue;
                    }
                    None => {
                        return Err(VoiceError::InvalidState(
                            "voice dave append proposals produced no commit",
                        ));
                    }
                };
                let (commit, _welcome) = local_external_sender
                    .split_commit_welcome(&commit_welcome)
                    .map_err(|_| VoiceError::InvalidState("voice dave commit welcome invalid"))?;
                let session = take_dave_session(&mut session)?;
                let runtime = complete_local_initial_creator_transition(
                    gateway,
                    session,
                    &commit_welcome,
                    &commit,
                )
                .await?;
                return Ok(initial_dave_state(
                    runtime,
                    group_id,
                    local_external_sender,
                    recognized_user_ids,
                ));
            }
            VoiceGatewayEvent::DaveMlsPrepareCommitTransition(DaveMlsPrepareCommitTransition {
                transition_id,
                commit,
            }) => {
                let runtime_protocol_version = if let Some(runtime_protocol_version) =
                    pending_prepared_transitions.remove(&transition_id)
                {
                    runtime_protocol_version
                } else if pending_key_package && pending_prepared_transitions.is_empty() {
                    protocol_version
                } else {
                    return Err(VoiceError::InvalidState(
                        "voice dave commit transition missing pending group creation",
                    ));
                };
                let commit_joined_group = {
                    let commit_result = dave_session_mut(&mut session)?
                        .process_commit(&commit)
                        .map_err(|_| VoiceError::InvalidState("voice dave commit invalid"))?;
                    if commit_result.is_ignored() {
                        None
                    } else {
                        Some(
                            !commit_result.is_failed()
                                && !commit_result.roster_member_ids().is_empty(),
                        )
                    }
                };
                tracing::debug!(
                    transition_id,
                    commit_len = commit.len(),
                    commit_ignored = commit_joined_group.is_none(),
                    commit_joined_group = commit_joined_group.unwrap_or(false),
                    runtime_protocol_version,
                    "voice dave handshake processed commit transition"
                );
                let Some(commit_joined_group) = commit_joined_group else {
                    continue;
                };
                if !commit_joined_group {
                    return Err(VoiceError::InvalidState(
                        "voice dave commit transition did not join group",
                    ));
                }
                pending_key_package = false;
                pending_prepared_transitions.clear();
                let runtime = DaveRuntimeContext::from_session(take_dave_session(&mut session)?)
                    .map_err(|_| VoiceError::InvalidState("voice dave runtime create failed"))?;
                if transition_id == DAVE_PROTOCOL_INIT_TRANSITION_ID {
                    tracing::debug!(
                        transition_id,
                        "voice dave handshake sending transition-ready for init transition"
                    );
                    gateway.send_dave_transition_ready(transition_id).await?;
                    return Ok(initial_dave_state(
                        runtime,
                        group_id,
                        local_external_sender,
                        recognized_user_ids,
                    ));
                }
                tracing::debug!(
                    transition_id,
                    "voice dave handshake sending transition-ready after commit transition"
                );
                gateway.send_dave_transition_ready(transition_id).await?;
                pending_transition = Some((transition_id, runtime));
            }
            VoiceGatewayEvent::DaveMlsWelcome(DaveMlsWelcome {
                transition_id,
                welcome,
            }) => {
                let runtime_protocol_version = if let Some(runtime_protocol_version) =
                    pending_prepared_transitions.remove(&transition_id)
                {
                    runtime_protocol_version
                } else if pending_key_package && pending_prepared_transitions.is_empty() {
                    protocol_version
                } else {
                    return Err(VoiceError::InvalidState(
                        "voice dave welcome transition missing pending join",
                    ));
                };
                let recognized = recognized_user_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                tracing::debug!(
                    transition_id,
                    welcome_len = welcome.len(),
                    recognized_user_ids = recognized.len(),
                    runtime_protocol_version,
                    "voice dave handshake processing welcome"
                );
                dave_session_mut(&mut session)?
                    .process_welcome(&welcome, &recognized)
                    .map_err(|_| VoiceError::InvalidState("voice dave welcome invalid"))?;
                pending_key_package = false;
                pending_prepared_transitions.clear();
                let runtime = DaveRuntimeContext::from_session(take_dave_session(&mut session)?)
                    .map_err(|_| VoiceError::InvalidState("voice dave runtime create failed"))?;
                if transition_id == DAVE_PROTOCOL_INIT_TRANSITION_ID {
                    tracing::debug!(
                        transition_id,
                        "voice dave handshake sending transition-ready for init transition"
                    );
                    gateway.send_dave_transition_ready(transition_id).await?;
                    return Ok(initial_dave_state(
                        runtime,
                        group_id,
                        local_external_sender,
                        recognized_user_ids,
                    ));
                }
                tracing::debug!(
                    transition_id,
                    "voice dave handshake sending transition-ready after welcome"
                );
                gateway.send_dave_transition_ready(transition_id).await?;
                pending_transition = Some((transition_id, runtime));
            }
            _ => {}
        }
    }
}

fn initial_dave_state(
    runtime: DaveRuntimeContext,
    group_id: u64,
    external_sender: DaveExternalSender,
    recognized_user_ids: BTreeSet<String>,
) -> InitialDaveState {
    InitialDaveState {
        runtime,
        group_id,
        external_sender,
        recognized_user_ids,
    }
}

fn has_only_self_recognized_user(
    recognized_user_ids: &BTreeSet<String>,
    self_user_id: &str,
) -> bool {
    recognized_user_ids.len() == 1 && recognized_user_ids.contains(self_user_id)
}

fn empty_mls_proposals() -> [u8; 1] {
    // davey expects a TLS-encoded empty VLBytes payload for APPEND proposals.
    [0]
}

fn dave_session_mut(session: &mut Option<DaveSession>) -> Result<&mut DaveSession, VoiceError> {
    session.as_mut().ok_or(VoiceError::InvalidState(
        "voice dave session already moved to runtime",
    ))
}

fn take_dave_session(session: &mut Option<DaveSession>) -> Result<DaveSession, VoiceError> {
    session.take().ok_or(VoiceError::InvalidState(
        "voice dave session already moved to runtime",
    ))
}

async fn complete_local_initial_creator_transition(
    gateway: &VoiceGatewayClient,
    mut session: DaveSession,
    commit_welcome: &[u8],
    commit: &[u8],
) -> Result<DaveRuntimeContext, VoiceError> {
    tracing::debug!(
        commit_welcome_len = commit_welcome.len(),
        "voice dave handshake sending commit welcome"
    );
    gateway
        .send_binary(protocol::dave_mls_commit_welcome_payload(commit_welcome))
        .await?;
    let commit_joined_group = {
        let commit_result = session
            .process_commit(commit)
            .map_err(|_| VoiceError::InvalidState("voice dave local init commit invalid"))?;
        !commit_result.is_failed()
            && !commit_result.is_ignored()
            && !commit_result.roster_member_ids().is_empty()
    };
    if !commit_joined_group {
        return Err(VoiceError::InvalidState(
            "voice dave local init commit did not join group",
        ));
    }
    let runtime = DaveRuntimeContext::from_session(session)
        .map_err(|_| VoiceError::InvalidState("voice dave runtime create failed"))?;
    tracing::debug!(
        transition_id = DAVE_PROTOCOL_INIT_TRANSITION_ID,
        commit_len = commit.len(),
        "voice dave handshake sending transition-ready after local init creator commit"
    );
    gateway
        .send_dave_transition_ready(DAVE_PROTOCOL_INIT_TRANSITION_ID)
        .await?;
    Ok(runtime)
}

async fn send_pending_join_key_package(
    gateway: &VoiceGatewayClient,
    session: &mut DaveSession,
    pending_key_package: &mut bool,
) -> Result<(), VoiceError> {
    if *pending_key_package {
        tracing::debug!("voice dave handshake key package already sent; skipping resend");
        return Ok(());
    }

    let key_package = session
        .key_package()
        .map_err(|_| VoiceError::InvalidState("voice dave key package failed"))?;
    tracing::debug!(
        key_package_len = key_package.len(),
        "voice dave handshake sending key package"
    );
    gateway.send_dave_mls_key_package(&key_package).await?;
    *pending_key_package = true;
    Ok(())
}

fn dave_handshake_event_name(event: &VoiceGatewayEvent) -> &'static str {
    match event {
        VoiceGatewayEvent::Hello(_) => "hello",
        VoiceGatewayEvent::Ready(_) => "ready",
        VoiceGatewayEvent::SessionDescription(_) => "session_description",
        VoiceGatewayEvent::Speaking(_) => "speaking",
        VoiceGatewayEvent::HeartbeatAck(_) => "heartbeat_ack",
        VoiceGatewayEvent::ClientsConnect(_) => "clients_connect",
        VoiceGatewayEvent::Video(_) => "video",
        VoiceGatewayEvent::ClientDisconnect(_) => "client_disconnect",
        VoiceGatewayEvent::MediaSinkWants => "media_sink_wants",
        VoiceGatewayEvent::ClientFlags(_) => "client_flags",
        VoiceGatewayEvent::ClientPlatform(_) => "client_platform",
        VoiceGatewayEvent::DavePrepareTransition(_) => "dave_prepare_transition",
        VoiceGatewayEvent::DaveExecuteTransition(_) => "dave_execute_transition",
        VoiceGatewayEvent::DavePrepareEpoch(_) => "dave_prepare_epoch",
        VoiceGatewayEvent::DaveMlsExternalSenderPackage(_) => "dave_mls_external_sender_package",
        VoiceGatewayEvent::DaveMlsProposals(_) => "dave_mls_proposals",
        VoiceGatewayEvent::DaveMlsPrepareCommitTransition(_) => {
            "dave_mls_prepare_commit_transition"
        }
        VoiceGatewayEvent::DaveMlsWelcome(_) => "dave_mls_welcome",
        VoiceGatewayEvent::Resumed => "resumed",
    }
}

fn spawn_heartbeat_task(
    gateway: VoiceGatewayClient,
    heartbeat_interval_ms: u64,
) -> oneshot::Sender<()> {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let interval = Duration::from_millis(heartbeat_interval_ms.max(1));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                _ = sleep(interval) => {
                    if gateway.send_heartbeat().await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    shutdown_tx
}

async fn next_event(
    gateway: &VoiceGatewayClient,
    timeout_duration: Duration,
) -> Result<protocol::VoiceGatewayPayload, VoiceError> {
    timeout(timeout_duration, gateway.receive_event())
        .await
        .map_err(|_| VoiceError::InvalidState("voice handshake timed out"))?
}

fn post_hello_timeout(heartbeat_interval_ms: u64) -> Duration {
    let heartbeat_interval = Duration::from_millis(heartbeat_interval_ms.max(1));
    std::cmp::max(
        POST_HELLO_TIMEOUT_FLOOR,
        heartbeat_interval.saturating_mul(2),
    )
}

async fn resolve_udp_target(ready: &Ready) -> Result<SocketAddr, VoiceError> {
    if let Ok(ip) = ready.ip.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, ready.port));
    }

    lookup_host((ready.ip.as_str(), ready.port))
        .await?
        .next()
        .ok_or(VoiceError::InvalidState(
            "voice ready udp target unresolved",
        ))
}

#[cfg(test)]
mod tests {
    use super::gateway_url;

    #[test]
    fn gateway_url_normalizes_forwarded_voice_hosts() {
        assert_eq!(
            gateway_url("voice.example.discord.gg").unwrap(),
            Some("wss://voice.example.discord.gg".to_owned())
        );
    }

    #[test]
    fn gateway_url_keeps_loopback_endpoints_on_ws() {
        assert_eq!(
            gateway_url("127.0.0.1:9000").unwrap(),
            Some("ws://127.0.0.1:9000".to_owned())
        );
    }
}
