use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::{IpAddr, SocketAddr};

use davey::ProposalsOperationType;
use tokio::net::lookup_host;
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant, sleep, timeout};

use crate::dave::{DaveExternalSender, DaveRuntimeContext, DaveSession, unpack_commit_welcome};
use crate::error::VoiceError;
use crate::gateway::VoiceGatewayClient;
use crate::protocol::{
    self, ClientDisconnect, ClientsConnect, DaveExecuteTransition, DaveMlsExternalSenderPackage,
    DaveMlsPrepareCommitTransition, DaveMlsProposals, DaveMlsWelcome, DavePrepareEpoch, Hello,
    Ready, SessionDescription, Speaking, VoiceGatewayEvent,
};
use crate::session::VoiceContext;
use crate::udp::{DiscoveredUdpAddress, VoiceUdpTransport};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const POST_HELLO_TIMEOUT_FLOOR: Duration = Duration::from_secs(30);
const DAVE_PROTOCOL_INIT_TRANSITION_ID: u16 = 0;
const SELF_ONLY_INITIAL_GROUP_GRACE: Duration = Duration::from_secs(5);
const VOICE_GATEWAY_VERSION: &str = "8";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingHandshakeTransitionSource {
    CommitBacked,
    WelcomeBacked,
}

struct PendingHandshakeTransition {
    transition_id: u16,
    runtime: DaveRuntimeContext,
    source: PendingHandshakeTransitionSource,
}

struct VoiceBootstrap {
    gateway: VoiceGatewayClient,
    transport: VoiceUdpTransport,
    ssrc: u32,
    heartbeat_shutdown: oneshot::Sender<()>,
    session_description: SessionDescription,
    pre_session_recognized_user_ids: BTreeSet<String>,
    pre_session_saw_existing_speaker: bool,
    dave_timeout: Duration,
}

pub struct VoiceHandshakeResult {
    pub gateway: VoiceGatewayClient,
    pub transport: VoiceUdpTransport,
    pub ssrc: u32,
    pub heartbeat_shutdown: oneshot::Sender<()>,
    pub session_description: SessionDescription,
    pub dave: Option<InitialDaveState>,
}

pub struct PendingObserverHandshakeResult {
    pub gateway: VoiceGatewayClient,
    pub transport: VoiceUdpTransport,
    pub ssrc: u32,
    pub heartbeat_shutdown: oneshot::Sender<()>,
    pub session_description: SessionDescription,
    pub dave_timeout: Duration,
    pub dave: Option<PendingObserverDaveState>,
}

pub struct ResumedVoiceGateway {
    pub gateway: VoiceGatewayClient,
    pub heartbeat_shutdown: oneshot::Sender<()>,
}

pub struct PendingObserverDaveState {
    pub session: Option<DaveSession>,
    pub user_id: String,
    pub group_id: u64,
    pub protocol_version: u16,
    pub external_sender_bytes: Option<Vec<u8>>,
    pub recognized_user_ids: BTreeSet<String>,
    pub pending_prepared_transitions: BTreeMap<u16, u16>,
    invalidated_transition_ids: BTreeSet<u16>,
    pub pending_key_package: bool,
    pending_gateway_winner_after_local_proposals_commit: bool,
    pending_proposals_replay_start: usize,
    pending_proposals: Vec<(crate::dave::DaveMlsProposalsOperation, Vec<u8>)>,
    seeded_events: VecDeque<VoiceGatewayEvent>,
    gateway_updates: Vec<PendingObserverGatewayUpdate>,
    saw_existing_speaker: bool,
}

pub(crate) enum PendingObserverGatewayUpdate {
    Speaking { user_id: String, ssrc: u32 },
    ClientDisconnect { user_id: String },
}

pub(crate) struct PendingObserverReadyResult {
    pub runtime: DaveRuntimeContext,
    pub gateway_updates: Vec<PendingObserverGatewayUpdate>,
    pub recognized_user_ids: BTreeSet<String>,
    pub material: ObserverDaveMaterial,
    pub completed_transition_id: Option<u16>,
}

#[derive(Clone)]
pub(crate) struct ObserverDaveMaterial {
    pub group_id: u64,
    pub protocol_version: u16,
    pub external_sender_bytes: Vec<u8>,
}

pub struct InitialDaveState {
    pub runtime: Option<DaveRuntimeContext>,
    pub pending_initial_session: Option<DaveSession>,
    pub group_id: u64,
    pub protocol_version: u16,
    pub external_sender: DaveExternalSender,
    pub external_sender_bytes: Vec<u8>,
    pub recognized_user_ids: BTreeSet<String>,
    pub completed_welcome_backed_transition_ids: BTreeSet<u16>,
    pub completed_local_init_commit_transition_ids: BTreeSet<u16>,
}

struct CompletedDaveTransitions {
    welcome_backed: BTreeSet<u16>,
    local_init_commit: BTreeSet<u16>,
}

pub async fn connect(voice: &VoiceContext) -> Result<Option<VoiceHandshakeResult>, VoiceError> {
    connect_active_participant(voice).await
}

pub async fn connect_active_participant(
    voice: &VoiceContext,
) -> Result<Option<VoiceHandshakeResult>, VoiceError> {
    let Some(bootstrap) = bootstrap_voice_connection(voice).await? else {
        return Ok(None);
    };
    let VoiceBootstrap {
        gateway,
        transport,
        ssrc,
        heartbeat_shutdown,
        session_description,
        pre_session_recognized_user_ids,
        pre_session_saw_existing_speaker: _,
        dave_timeout,
    } = bootstrap;
    let dave = if let Some(protocol_version) = session_description.dave_protocol_version {
        Some(
            complete_active_dave_transition(
                &gateway,
                voice,
                ssrc,
                protocol_version,
                dave_timeout,
                pre_session_recognized_user_ids,
            )
            .await?,
        )
    } else {
        None
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

pub async fn connect_observer_participant(
    voice: &VoiceContext,
) -> Result<Option<PendingObserverHandshakeResult>, VoiceError> {
    let Some(bootstrap) = bootstrap_voice_connection(voice).await? else {
        return Ok(None);
    };
    let VoiceBootstrap {
        gateway,
        transport,
        ssrc,
        heartbeat_shutdown,
        session_description,
        pre_session_recognized_user_ids,
        pre_session_saw_existing_speaker,
        dave_timeout,
    } = bootstrap;
    let dave = if let Some(protocol_version) = session_description.dave_protocol_version {
        Some(
            start_pending_observer_dave_join(
                &gateway,
                voice,
                ssrc,
                protocol_version,
                pre_session_recognized_user_ids,
                pre_session_saw_existing_speaker,
            )
            .await?,
        )
    } else {
        None
    };

    Ok(Some(PendingObserverHandshakeResult {
        gateway,
        transport,
        ssrc,
        heartbeat_shutdown,
        session_description,
        dave_timeout,
        dave,
    }))
}

async fn bootstrap_voice_connection(
    voice: &VoiceContext,
) -> Result<Option<VoiceBootstrap>, VoiceError> {
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
    let dave_timeout = post_hello_timeout(hello.heartbeat_interval_ms);
    let heartbeat_shutdown = spawn_heartbeat_task(gateway.clone(), hello.heartbeat_interval_ms);

    let bootstrap_result = async {
        gateway.send_identify(voice).await?;

        let ready = expect_ready(&gateway, dave_timeout).await?;
        let mode = protocol::choose_encryption_mode(&ready)?.to_owned();
        let udp_target = resolve_udp_target(&ready).await?;
        let transport = VoiceUdpTransport::connect(udp_target, ready.ssrc).await?;
        let discovered = DiscoveredUdpAddress {
            ip: transport.local_addr().ip().to_string(),
            port: transport.local_addr().port(),
        };

        gateway.send_select_protocol(&discovered, &mode).await?;
        let (
            session_description,
            pre_session_recognized_user_ids,
            pre_session_saw_existing_speaker,
        ) = expect_session_description(&gateway, dave_timeout, &voice.user_id).await?;

        Ok::<_, VoiceError>((
            transport,
            ready.ssrc,
            session_description,
            pre_session_recognized_user_ids,
            pre_session_saw_existing_speaker,
        ))
    }
    .await;

    match bootstrap_result {
        Ok((
            transport,
            ssrc,
            session_description,
            pre_session_recognized_user_ids,
            pre_session_saw_existing_speaker,
        )) => Ok(Some(VoiceBootstrap {
            gateway,
            transport,
            ssrc,
            heartbeat_shutdown,
            session_description,
            pre_session_recognized_user_ids,
            pre_session_saw_existing_speaker,
            dave_timeout,
        })),
        Err(err) => {
            let _ = heartbeat_shutdown.send(());
            Err(err)
        }
    }
}

pub async fn resume(voice: &VoiceContext, seq_ack: Option<u64>) -> Result<(), VoiceError> {
    resume_gateway(voice, seq_ack).await.map(|_| ())
}

pub async fn resume_gateway(
    voice: &VoiceContext,
    seq_ack: Option<u64>,
) -> Result<ResumedVoiceGateway, VoiceError> {
    let Some(gateway_url) = gateway_url(&voice.endpoint)? else {
        return Err(VoiceError::InvalidState(
            "voice endpoint invalid for resume",
        ));
    };

    let gateway = timeout(HANDSHAKE_TIMEOUT, VoiceGatewayClient::connect(&gateway_url))
        .await
        .map_err(|_| VoiceError::InvalidState("voice gateway connect timed out"))??;
    let hello = expect_hello(&gateway).await?;
    tracing::debug!(
        heartbeat_interval_ms = hello.heartbeat_interval_ms,
        "voice handshake received hello for resume"
    );
    let post_hello_timeout = post_hello_timeout(hello.heartbeat_interval_ms);
    let heartbeat_shutdown = spawn_heartbeat_task(gateway.clone(), hello.heartbeat_interval_ms);

    let resume_result = async {
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
    .await;

    match resume_result {
        Ok(()) => Ok(ResumedVoiceGateway {
            gateway,
            heartbeat_shutdown,
        }),
        Err(err) => {
            let _ = heartbeat_shutdown.send(());
            Err(err)
        }
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
        return Ok(Some(format!("ws://{trimmed}/?v={VOICE_GATEWAY_VERSION}")));
    }

    if looks_like_forwarded_endpoint(trimmed)? {
        return Ok(Some(format!("wss://{trimmed}/?v={VOICE_GATEWAY_VERSION}")));
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

    parts.path_and_query = Some(versioned_path_and_query(uri)?);

    Ok(http::Uri::from_parts(parts)
        .map_err(|_| VoiceError::InvalidState("voice endpoint could not be normalized"))?
        .to_string())
}

fn versioned_path_and_query(uri: &http::Uri) -> Result<http::uri::PathAndQuery, VoiceError> {
    let path_and_query = uri
        .path_and_query()
        .map(http::uri::PathAndQuery::as_str)
        .unwrap_or("/");
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    let path = if path.is_empty() { "/" } else { path };

    let version_present = query.split('&').any(|part| {
        part.split_once('=')
            .map(|(key, _)| key == "v")
            .unwrap_or(part == "v")
    });
    let versioned = if version_present {
        format!("{path}?{query}")
    } else if query.is_empty() {
        format!("{path}?v={VOICE_GATEWAY_VERSION}")
    } else {
        format!("{path}?{query}&v={VOICE_GATEWAY_VERSION}")
    };

    versioned
        .parse()
        .map_err(|_| VoiceError::InvalidState("voice endpoint query invalid"))
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
) -> Result<(SessionDescription, BTreeSet<String>, bool), VoiceError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut recognized_user_ids = BTreeSet::from([self_user_id.to_owned()]);
    let mut saw_existing_speaker = false;

    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(VoiceError::InvalidState("voice handshake timed out"))?;

        match next_event(gateway, remaining).await?.into_event() {
            VoiceGatewayEvent::SessionDescription(description) => {
                return Ok((description, recognized_user_ids, saw_existing_speaker));
            }
            VoiceGatewayEvent::ClientsConnect(ClientsConnect { user_ids }) => {
                let user_count = user_ids.len();
                recognized_user_ids.extend(user_ids);
                tracing::debug!(
                    user_count,
                    recognized_user_ids = recognized_user_ids.len(),
                    "voice handshake recorded pre-session connected users"
                );
            }
            VoiceGatewayEvent::ClientDisconnect(ClientDisconnect { user_id })
                if user_id != self_user_id =>
            {
                recognized_user_ids.remove(&user_id);
                tracing::debug!(
                    %user_id,
                    recognized_user_ids = recognized_user_ids.len(),
                    "voice handshake recorded pre-session disconnect"
                );
            }
            VoiceGatewayEvent::Speaking(Speaking {
                user_id: Some(user_id),
                ..
            }) if user_id != self_user_id => {
                saw_existing_speaker = true;
                tracing::debug!(
                    %user_id,
                    "voice handshake recorded pre-session existing speaker"
                );
            }
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

async fn complete_active_dave_transition(
    gateway: &VoiceGatewayClient,
    voice: &VoiceContext,
    ssrc: u32,
    protocol_version: u16,
    timeout_duration: Duration,
    mut recognized_user_ids: BTreeSet<String>,
) -> Result<InitialDaveState, VoiceError> {
    let handshake_deadline = Instant::now() + timeout_duration;
    let group_id = voice
        .channel_id
        .parse::<u64>()
        .map_err(|_| VoiceError::InvalidState("voice dave group id invalid"))?;
    let local_external_sender = DaveExternalSender::new(group_id)
        .map_err(|_| VoiceError::InvalidState("voice dave external sender create failed"))?;
    recognized_user_ids.insert(voice.user_id.clone());
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
    let mut pending_transition = None::<PendingHandshakeTransition>;
    let mut pending_key_package = false;
    let mut completed_welcome_backed_transition_ids = BTreeSet::new();
    let mut self_only_group_deadline = None::<Instant>;
    let mut pending_local_init_transition = None::<(DaveRuntimeContext, Vec<u8>)>;
    let mut gateway_external_sender = None::<Vec<u8>>;
    send_pending_join_key_package(
        gateway,
        dave_session_mut(&mut session)?,
        &mut pending_key_package,
    )
    .await?;

    loop {
        let now = Instant::now();
        let remaining_handshake = handshake_deadline
            .checked_duration_since(now)
            .ok_or(VoiceError::InvalidState("voice handshake timed out"))?;
        let wait_duration = self_only_group_deadline
            .map(|deadline| remaining_handshake.min(deadline.saturating_duration_since(now)))
            .unwrap_or(remaining_handshake);
        let event = match timeout(wait_duration, gateway.receive_event()).await {
            Ok(result) => result?.into_event(),
            Err(_) if self_only_group_deadline.is_some() => {
                tracing::debug!(
                    recognized_user_ids = recognized_user_ids.len(),
                    "voice dave handshake keeping initial session pending after grace window"
                );
                let pending_session = take_dave_session(&mut session)?;
                return Ok(pending_initial_dave_state(
                    pending_session,
                    group_id,
                    protocol_version,
                    current_external_sender_bytes(
                        &gateway_external_sender,
                        &local_external_sender,
                    )?,
                    local_external_sender,
                    recognized_user_ids,
                    completed_welcome_backed_transition_ids,
                ));
            }
            Err(_) => return Err(VoiceError::InvalidState("voice handshake timed out")),
        };
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
                        .is_some_and(|pending_transition| {
                            *transition_id == pending_transition.transition_id
                        }),
                "voice dave handshake processing execute transition"
            );
            if pending_transition
                .as_ref()
                .is_some_and(|pending_transition| {
                    *transition_id == pending_transition.transition_id
                })
            {
                let PendingHandshakeTransition {
                    transition_id: completed_transition_id,
                    runtime,
                    source,
                } = pending_transition.take().expect("pending transition");
                if source == PendingHandshakeTransitionSource::WelcomeBacked {
                    completed_welcome_backed_transition_ids.insert(completed_transition_id);
                }
                let completed_local_init_commit_transition_ids = if completed_transition_id
                    == DAVE_PROTOCOL_INIT_TRANSITION_ID
                    && source == PendingHandshakeTransitionSource::CommitBacked
                {
                    BTreeSet::from([completed_transition_id])
                } else {
                    BTreeSet::new()
                };
                return Ok(initial_dave_state(
                    runtime,
                    group_id,
                    protocol_version,
                    current_external_sender_bytes(
                        &gateway_external_sender,
                        &local_external_sender,
                    )?,
                    local_external_sender,
                    recognized_user_ids,
                    CompletedDaveTransitions {
                        welcome_backed: completed_welcome_backed_transition_ids,
                        local_init_commit: completed_local_init_commit_transition_ids,
                    },
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
                gateway_external_sender = Some(sender);
                tracing::debug!(
                    grace_ms = SELF_ONLY_INITIAL_GROUP_GRACE.as_millis(),
                    recognized_user_ids = recognized_user_ids.len(),
                    "voice dave handshake deferring initial group until no peer DAVE events arrive"
                );
                self_only_group_deadline = Some(Instant::now() + SELF_ONLY_INITIAL_GROUP_GRACE);
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
                if pending_local_init_transition.is_some() {
                    if let Some(transition_id) = transition_id {
                        pending_prepared_transitions
                            .insert(transition_id, prepare_protocol_version);
                    }
                    continue;
                }
                self_only_group_deadline = None;
                send_pending_join_key_package(
                    gateway,
                    dave_session_mut(&mut session)?,
                    &mut pending_key_package,
                )
                .await?;
                if let Some(transition_id) = transition_id {
                    pending_prepared_transitions.insert(transition_id, prepare_protocol_version);
                }
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
                if pending_local_init_transition.is_some() {
                    tracing::debug!(
                        proposals_len = proposals.len(),
                        recognized_user_ids = recognized.len(),
                        "voice dave handshake ignoring follow-up proposals while waiting for gateway local init commit"
                    );
                    continue;
                }
                let runtime = complete_initial_proposals_transition(
                    gateway,
                    &mut session,
                    &local_external_sender,
                    operation,
                    &proposals,
                    &recognized_user_ids,
                )
                .await?;
                let Some(runtime) = runtime else {
                    tracing::debug!(
                        operation = ?ProposalsOperationType::from(operation),
                        "voice dave handshake ignoring no-op proposals update"
                    );
                    continue;
                };
                tracing::debug!(
                    transition_id = DAVE_PROTOCOL_INIT_TRANSITION_ID,
                    "voice dave handshake completed local init creator commit"
                );
                let (runtime, _local_commit) = runtime;
                return Ok(initial_dave_state(
                    runtime,
                    group_id,
                    protocol_version,
                    current_external_sender_bytes(
                        &gateway_external_sender,
                        &local_external_sender,
                    )?,
                    local_external_sender,
                    recognized_user_ids,
                    CompletedDaveTransitions {
                        welcome_backed: completed_welcome_backed_transition_ids,
                        local_init_commit: BTreeSet::from([DAVE_PROTOCOL_INIT_TRANSITION_ID]),
                    },
                ));
            }
            VoiceGatewayEvent::DaveMlsPrepareCommitTransition(DaveMlsPrepareCommitTransition {
                transition_id,
                commit,
            }) => {
                if transition_id == DAVE_PROTOCOL_INIT_TRANSITION_ID
                    && let Some((runtime, local_commit)) = pending_local_init_transition.take()
                {
                    if commit != local_commit {
                        return Err(VoiceError::InvalidState(
                            "voice dave local init gateway commit mismatch",
                        ));
                    }
                    tracing::debug!(
                        transition_id,
                        commit_len = commit.len(),
                        "voice dave handshake confirmed gateway local init commit"
                    );
                    pending_prepared_transitions.clear();
                    return Ok(initial_dave_state(
                        runtime,
                        group_id,
                        protocol_version,
                        current_external_sender_bytes(
                            &gateway_external_sender,
                            &local_external_sender,
                        )?,
                        local_external_sender,
                        recognized_user_ids,
                        CompletedDaveTransitions {
                            welcome_backed: completed_welcome_backed_transition_ids,
                            local_init_commit: BTreeSet::from([transition_id]),
                        },
                    ));
                }
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
                self_only_group_deadline = None;
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
                        "voice dave handshake completed init commit transition"
                    );
                    return Ok(initial_dave_state(
                        runtime,
                        group_id,
                        protocol_version,
                        current_external_sender_bytes(
                            &gateway_external_sender,
                            &local_external_sender,
                        )?,
                        local_external_sender,
                        recognized_user_ids,
                        CompletedDaveTransitions {
                            welcome_backed: completed_welcome_backed_transition_ids,
                            local_init_commit: BTreeSet::new(),
                        },
                    ));
                }
                tracing::debug!(
                    transition_id,
                    "voice dave handshake sending transition-ready after commit transition"
                );
                gateway.send_dave_transition_ready(transition_id).await?;
                pending_transition = Some(PendingHandshakeTransition {
                    transition_id,
                    runtime,
                    source: PendingHandshakeTransitionSource::CommitBacked,
                });
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
                self_only_group_deadline = None;
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
                        "voice dave handshake completed init welcome transition"
                    );
                    return Ok(initial_dave_state(
                        runtime,
                        group_id,
                        protocol_version,
                        current_external_sender_bytes(
                            &gateway_external_sender,
                            &local_external_sender,
                        )?,
                        local_external_sender,
                        recognized_user_ids,
                        CompletedDaveTransitions {
                            welcome_backed: completed_welcome_backed_transition_ids,
                            local_init_commit: BTreeSet::new(),
                        },
                    ));
                }
                tracing::debug!(
                    transition_id,
                    "voice dave handshake sending transition-ready after welcome"
                );
                gateway.send_dave_transition_ready(transition_id).await?;
                pending_transition = Some(PendingHandshakeTransition {
                    transition_id,
                    runtime,
                    source: PendingHandshakeTransitionSource::WelcomeBacked,
                });
            }
            _ => {}
        }
    }
}

async fn start_pending_observer_dave_join(
    gateway: &VoiceGatewayClient,
    voice: &VoiceContext,
    ssrc: u32,
    protocol_version: u16,
    mut recognized_user_ids: BTreeSet<String>,
    pre_session_saw_existing_speaker: bool,
) -> Result<PendingObserverDaveState, VoiceError> {
    let group_id = voice
        .channel_id
        .parse::<u64>()
        .map_err(|_| VoiceError::InvalidState("voice dave group id invalid"))?;
    recognized_user_ids.insert(voice.user_id.clone());
    let mut pending = PendingObserverDaveState {
        session: Some(
            DaveSession::new(None)
                .map_err(|_| VoiceError::InvalidState("voice dave session create failed"))?,
        ),
        user_id: voice.user_id.clone(),
        group_id,
        protocol_version,
        external_sender_bytes: None,
        recognized_user_ids,
        pending_prepared_transitions: BTreeMap::new(),
        invalidated_transition_ids: BTreeSet::new(),
        pending_key_package: false,
        pending_gateway_winner_after_local_proposals_commit: false,
        pending_proposals_replay_start: 0,
        pending_proposals: Vec::new(),
        seeded_events: VecDeque::new(),
        gateway_updates: Vec::new(),
        saw_existing_speaker: pre_session_saw_existing_speaker,
    };
    dave_session_mut(&mut pending.session)?
        .init(protocol_version, group_id, &voice.user_id)
        .map_err(|_| VoiceError::InvalidState("voice dave session init failed"))?;
    tracing::debug!(
        group_id,
        protocol_version,
        user_id = %voice.user_id,
        ssrc,
        "voice observer dave handshake initialized session"
    );
    send_pending_join_key_package(
        gateway,
        dave_session_mut(&mut pending.session)?,
        &mut pending.pending_key_package,
    )
    .await?;
    seed_pending_observer_dave_state(gateway, &mut pending).await?;
    Ok(pending)
}

async fn seed_pending_observer_dave_state(
    gateway: &VoiceGatewayClient,
    pending: &mut PendingObserverDaveState,
) -> Result<(), VoiceError> {
    let protocol_version = observer_dave_protocol_version(pending)?;
    loop {
        let event = match timeout(Duration::ZERO, gateway.receive_event()).await {
            Ok(Ok(payload)) => payload.into_event(),
            Ok(Err(err)) => return Err(err),
            Err(_) => return Ok(()),
        };
        tracing::debug!(
            event = dave_handshake_event_name(&event),
            recognized_user_ids = pending.recognized_user_ids.len(),
            pending_prepared_transitions = pending.pending_prepared_transitions.len(),
            "voice observer dave handshake draining seed event"
        );

        match event {
            VoiceGatewayEvent::Speaking(Speaking {
                user_id: Some(user_id),
                ssrc,
                ..
            }) => {
                pending.saw_existing_speaker = true;
                pending
                    .gateway_updates
                    .push(PendingObserverGatewayUpdate::Speaking { user_id, ssrc });
            }
            VoiceGatewayEvent::ClientDisconnect(ClientDisconnect { user_id }) => {
                pending
                    .gateway_updates
                    .push(PendingObserverGatewayUpdate::ClientDisconnect { user_id });
            }
            VoiceGatewayEvent::ClientsConnect(ClientsConnect { user_ids }) => {
                pending.recognized_user_ids.extend(user_ids);
            }
            VoiceGatewayEvent::DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage {
                external_sender: sender,
            }) => {
                tracing::debug!(
                    external_sender_len = sender.len(),
                    "voice observer dave handshake seeding external sender"
                );
                dave_session_mut(&mut pending.session)?
                    .set_external_sender(&sender)
                    .map_err(|_| VoiceError::InvalidState("voice dave external sender invalid"))?;
                pending.external_sender_bytes = Some(sender);
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
                    "voice observer dave handshake seeding prepare epoch"
                );
                if let Some(transition_id) = transition_id {
                    pending
                        .pending_prepared_transitions
                        .insert(transition_id, prepare_protocol_version);
                }
            }
            event @ (VoiceGatewayEvent::DaveMlsProposals(_)
            | VoiceGatewayEvent::DaveMlsPrepareCommitTransition(_)
            | VoiceGatewayEvent::DaveMlsWelcome(_)) => {
                pending.seeded_events.push_back(event);
                return Ok(());
            }
            _ => {}
        }
    }
}

pub(crate) async fn complete_pending_observer_dave_join(
    gateway: &VoiceGatewayClient,
    mut pending: PendingObserverDaveState,
    timeout_duration: Duration,
    author_proposals: bool,
) -> Result<PendingObserverReadyResult, VoiceError> {
    let handshake_deadline = Instant::now() + timeout_duration;
    let mut gateway_updates = std::mem::take(&mut pending.gateway_updates);

    loop {
        let event = if let Some(event) = pending.seeded_events.pop_front() {
            event
        } else {
            let remaining = handshake_deadline
                .checked_duration_since(Instant::now())
                .ok_or(VoiceError::InvalidState(
                    "voice observer dave join timed out",
                ))?;
            timeout(remaining, gateway.receive_event())
                .await
                .map_err(|_| VoiceError::InvalidState("voice observer dave join timed out"))??
                .into_event()
        };
        tracing::debug!(
            event = dave_handshake_event_name(&event),
            transition_id = dave_handshake_event_transition_id(&event),
            pending_key_package = pending.pending_key_package,
            pending_prepared_transitions = pending.pending_prepared_transitions.len(),
            recognized_user_ids = pending.recognized_user_ids.len(),
            "voice observer dave handshake received gateway event"
        );

        match event {
            VoiceGatewayEvent::Speaking(Speaking {
                user_id: Some(user_id),
                ssrc,
                ..
            }) => {
                pending.saw_existing_speaker = true;
                gateway_updates.push(PendingObserverGatewayUpdate::Speaking { user_id, ssrc });
            }
            VoiceGatewayEvent::ClientDisconnect(ClientDisconnect { user_id }) => {
                gateway_updates.push(PendingObserverGatewayUpdate::ClientDisconnect { user_id });
            }
            VoiceGatewayEvent::ClientsConnect(ClientsConnect { user_ids }) => {
                pending.recognized_user_ids.extend(user_ids);
            }
            VoiceGatewayEvent::DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage {
                external_sender: sender,
            }) => {
                tracing::debug!(
                    external_sender_len = sender.len(),
                    "voice observer dave handshake received external sender"
                );
                dave_session_mut(&mut pending.session)?
                    .set_external_sender(&sender)
                    .map_err(|_| VoiceError::InvalidState("voice dave external sender invalid"))?;
                pending.external_sender_bytes = Some(sender);
            }
            VoiceGatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
                transition_id,
                epoch,
                protocol_version: prepare_protocol_version,
            }) => {
                if transition_id.is_some_and(|transition_id| {
                    pending.invalidated_transition_ids.contains(&transition_id)
                }) {
                    tracing::debug!(
                        transition_id,
                        "voice observer dave handshake ignoring prepare epoch for invalidated transition"
                    );
                    continue;
                }
                let protocol_version = observer_dave_protocol_version(&pending)?;
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
                    "voice observer dave handshake processing prepare epoch"
                );
                if let Some(transition_id) = transition_id {
                    pending
                        .pending_prepared_transitions
                        .insert(transition_id, prepare_protocol_version);
                }
                send_pending_join_key_package(
                    gateway,
                    dave_session_mut(&mut pending.session)?,
                    &mut pending.pending_key_package,
                )
                .await?;
            }
            VoiceGatewayEvent::DaveMlsProposals(DaveMlsProposals {
                operation,
                proposals,
            }) => {
                if !pending.pending_key_package {
                    return Err(VoiceError::InvalidState(
                        "voice dave proposals missing pending group creation",
                    ));
                }
                tracing::debug!(
                    proposals_len = proposals.len(),
                    recognized_user_ids = pending.recognized_user_ids.len(),
                    operation = ?ProposalsOperationType::from(operation),
                    pending_gateway_winner_after_local_proposals_commit =
                        pending.pending_gateway_winner_after_local_proposals_commit,
                    author_proposals,
                    "voice observer dave handshake processing proposals"
                );
                pending
                    .pending_proposals
                    .push((operation, proposals.clone()));
                if !author_proposals && !pending.pending_gateway_winner_after_local_proposals_commit
                {
                    pending.pending_gateway_winner_after_local_proposals_commit = true;
                    pending.pending_proposals_replay_start = pending.pending_proposals.len();
                    tracing::debug!(
                        proposals_len = proposals.len(),
                        recognized_user_ids = pending.recognized_user_ids.len(),
                        operation = ?ProposalsOperationType::from(operation),
                        "voice observer dave handshake waiting for sender-authored transition because local proposal authoring is disabled"
                    );
                }
                if pending.saw_existing_speaker
                    && !pending.pending_gateway_winner_after_local_proposals_commit
                {
                    pending.pending_gateway_winner_after_local_proposals_commit = true;
                    pending.pending_proposals_replay_start = pending.pending_proposals.len();
                    tracing::debug!(
                        proposals_len = proposals.len(),
                        recognized_user_ids = pending.recognized_user_ids.len(),
                        operation = ?ProposalsOperationType::from(operation),
                        "voice observer dave handshake waiting for sender-authored transition after seeing active speaker"
                    );
                }
                if pending.pending_gateway_winner_after_local_proposals_commit {
                    tracing::debug!(
                        proposals_len = proposals.len(),
                        recognized_user_ids = pending.recognized_user_ids.len(),
                        operation = ?ProposalsOperationType::from(operation),
                        "voice observer dave handshake buffering follow-up proposals while waiting for gateway winner"
                    );
                    continue;
                }
                let commit_welcome = process_initial_proposals(
                    &mut pending.session,
                    operation,
                    &proposals,
                    &pending.recognized_user_ids,
                )?;
                let Some(commit_welcome) = commit_welcome else {
                    tracing::debug!(
                        operation = ?ProposalsOperationType::from(operation),
                        "voice observer dave handshake ignoring no-op proposals update"
                    );
                    continue;
                };
                let (commit, _welcome) = unpack_commit_welcome(&commit_welcome)
                    .map_err(|_| VoiceError::InvalidState("voice dave commit welcome invalid"))?;
                let session = take_dave_session(&mut pending.session)?;
                let runtime = complete_local_initial_creator_transition(
                    gateway,
                    session,
                    &commit_welcome,
                    &commit,
                    false,
                )
                .await?;
                pending.pending_gateway_winner_after_local_proposals_commit = true;
                pending.pending_proposals_replay_start = pending.pending_proposals.len();
                tracing::debug!(
                    recognized_user_ids = pending.recognized_user_ids.len(),
                    "voice observer dave handshake completed local proposals commit"
                );
                return Ok(PendingObserverReadyResult {
                    runtime,
                    gateway_updates,
                    material: observer_dave_material(&pending)?,
                    recognized_user_ids: pending.recognized_user_ids,
                    completed_transition_id: Some(DAVE_PROTOCOL_INIT_TRANSITION_ID),
                });
            }
            VoiceGatewayEvent::DaveMlsPrepareCommitTransition(DaveMlsPrepareCommitTransition {
                transition_id,
                commit,
            }) => {
                if pending.invalidated_transition_ids.contains(&transition_id) {
                    pending.pending_prepared_transitions.remove(&transition_id);
                    tracing::debug!(
                        transition_id,
                        commit_len = commit.len(),
                        "voice observer dave handshake ignoring commit for invalidated transition"
                    );
                    continue;
                }
                let protocol_version = observer_dave_protocol_version(&pending)?;
                let runtime_protocol_version = if let Some(runtime_protocol_version) =
                    pending.pending_prepared_transitions.remove(&transition_id)
                {
                    runtime_protocol_version
                } else if pending.pending_key_package
                    && pending.pending_prepared_transitions.is_empty()
                {
                    protocol_version
                } else {
                    return Err(VoiceError::InvalidState(
                        "voice observer dave commit transition missing pending join",
                    ));
                };
                let commit_result =
                    match dave_session_mut(&mut pending.session)?.process_commit(&commit) {
                        Ok(commit_result) => commit_result,
                        Err(err) => {
                            tracing::debug!(
                                transition_id,
                                commit_len = commit.len(),
                                error = ?err,
                                "voice observer dave handshake reinitializing after invalid commit"
                            );
                            restart_pending_observer_dave_join_after_invalid_transition(
                                gateway,
                                &mut pending,
                                transition_id,
                            )
                            .await?;
                            continue;
                        }
                    };
                tracing::debug!(
                    transition_id,
                    commit_len = commit.len(),
                    commit_failed = commit_result.is_failed(),
                    commit_ignored = commit_result.is_ignored(),
                    roster_member_ids = commit_result.roster_member_ids().len(),
                    runtime_protocol_version,
                    "voice observer dave handshake processed commit transition"
                );
                if commit_result.is_failed()
                    || commit_result.is_ignored()
                    || commit_result.roster_member_ids().is_empty()
                {
                    tracing::debug!(
                        transition_id,
                        commit_failed = commit_result.is_failed(),
                        commit_ignored = commit_result.is_ignored(),
                        roster_member_ids = commit_result.roster_member_ids().len(),
                        "voice observer dave handshake reinitializing after rejected commit"
                    );
                    restart_pending_observer_dave_join_after_invalid_transition(
                        gateway,
                        &mut pending,
                        transition_id,
                    )
                    .await?;
                    continue;
                }
                let mut runtime =
                    DaveRuntimeContext::from_session(take_dave_session(&mut pending.session)?)
                        .map_err(|_| {
                            VoiceError::InvalidState("voice dave runtime create failed")
                        })?;
                stage_pending_observer_proposals(
                    &mut runtime,
                    &pending,
                    pending.pending_proposals_replay_start,
                )?;
                if transition_id != DAVE_PROTOCOL_INIT_TRANSITION_ID {
                    gateway.send_dave_transition_ready(transition_id).await?;
                }
                return Ok(PendingObserverReadyResult {
                    runtime,
                    gateway_updates,
                    material: observer_dave_material(&pending)?,
                    recognized_user_ids: pending.recognized_user_ids,
                    completed_transition_id: Some(transition_id),
                });
            }
            VoiceGatewayEvent::DaveMlsWelcome(DaveMlsWelcome {
                transition_id,
                welcome,
            }) => {
                if pending.invalidated_transition_ids.contains(&transition_id) {
                    pending.pending_prepared_transitions.remove(&transition_id);
                    tracing::debug!(
                        transition_id,
                        welcome_len = welcome.len(),
                        "voice observer dave handshake ignoring welcome for invalidated transition"
                    );
                    continue;
                }
                let protocol_version = observer_dave_protocol_version(&pending)?;
                let runtime_protocol_version = if let Some(runtime_protocol_version) =
                    pending.pending_prepared_transitions.remove(&transition_id)
                {
                    runtime_protocol_version
                } else if pending.pending_key_package
                    && pending.pending_prepared_transitions.is_empty()
                {
                    protocol_version
                } else {
                    return Err(VoiceError::InvalidState(
                        "voice observer dave welcome transition missing pending join",
                    ));
                };
                let recognized = pending
                    .recognized_user_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                tracing::debug!(
                    transition_id,
                    welcome_len = welcome.len(),
                    recognized_user_ids = recognized.len(),
                    runtime_protocol_version,
                    "voice observer dave handshake processing welcome"
                );
                if let Err(err) =
                    dave_session_mut(&mut pending.session)?.process_welcome(&welcome, &recognized)
                {
                    tracing::debug!(
                        transition_id,
                        welcome_len = welcome.len(),
                        error = ?err,
                        "voice observer dave handshake reinitializing after invalid welcome"
                    );
                    restart_pending_observer_dave_join_after_invalid_transition(
                        gateway,
                        &mut pending,
                        transition_id,
                    )
                    .await?;
                    continue;
                }
                let mut runtime =
                    DaveRuntimeContext::from_session(take_dave_session(&mut pending.session)?)
                        .map_err(|_| {
                            VoiceError::InvalidState("voice dave runtime create failed")
                        })?;
                stage_pending_observer_proposals(
                    &mut runtime,
                    &pending,
                    pending.pending_proposals_replay_start,
                )?;
                if transition_id != DAVE_PROTOCOL_INIT_TRANSITION_ID {
                    gateway.send_dave_transition_ready(transition_id).await?;
                }
                return Ok(PendingObserverReadyResult {
                    runtime,
                    gateway_updates,
                    material: observer_dave_material(&pending)?,
                    recognized_user_ids: pending.recognized_user_ids,
                    completed_transition_id: Some(transition_id),
                });
            }
            _ => {}
        }
    }
}

fn initial_dave_state(
    runtime: DaveRuntimeContext,
    group_id: u64,
    protocol_version: u16,
    external_sender_bytes: Vec<u8>,
    external_sender: DaveExternalSender,
    recognized_user_ids: BTreeSet<String>,
    completed_transitions: CompletedDaveTransitions,
) -> InitialDaveState {
    InitialDaveState {
        runtime: Some(runtime),
        pending_initial_session: None,
        group_id,
        protocol_version,
        external_sender,
        external_sender_bytes,
        recognized_user_ids,
        completed_welcome_backed_transition_ids: completed_transitions.welcome_backed,
        completed_local_init_commit_transition_ids: completed_transitions.local_init_commit,
    }
}

fn pending_initial_dave_state(
    session: DaveSession,
    group_id: u64,
    protocol_version: u16,
    external_sender_bytes: Vec<u8>,
    external_sender: DaveExternalSender,
    recognized_user_ids: BTreeSet<String>,
    completed_welcome_backed_transition_ids: BTreeSet<u16>,
) -> InitialDaveState {
    InitialDaveState {
        runtime: None,
        pending_initial_session: Some(session),
        group_id,
        protocol_version,
        external_sender,
        external_sender_bytes,
        recognized_user_ids,
        completed_welcome_backed_transition_ids,
        completed_local_init_commit_transition_ids: BTreeSet::new(),
    }
}

fn current_external_sender_bytes(
    gateway_external_sender: &Option<Vec<u8>>,
    local_external_sender: &DaveExternalSender,
) -> Result<Vec<u8>, VoiceError> {
    match gateway_external_sender {
        Some(sender) => Ok(sender.clone()),
        None => local_external_sender
            .marshalled_external_sender()
            .map_err(|_| VoiceError::InvalidState("voice dave external sender unavailable")),
    }
}

pub(crate) async fn reinitialize_pending_observer_dave_join_after_invalid_transition(
    gateway: &VoiceGatewayClient,
    voice: &VoiceContext,
    material: &ObserverDaveMaterial,
    mut recognized_user_ids: BTreeSet<String>,
    invalid_transition_id: u16,
) -> Result<PendingObserverDaveState, VoiceError> {
    recognized_user_ids.insert(voice.user_id.clone());
    let mut pending = PendingObserverDaveState {
        session: Some(
            DaveSession::new(None)
                .map_err(|_| VoiceError::InvalidState("voice dave session create failed"))?,
        ),
        user_id: voice.user_id.clone(),
        group_id: material.group_id,
        protocol_version: material.protocol_version,
        external_sender_bytes: Some(material.external_sender_bytes.clone()),
        recognized_user_ids,
        pending_prepared_transitions: BTreeMap::new(),
        invalidated_transition_ids: BTreeSet::from([invalid_transition_id]),
        pending_key_package: false,
        pending_gateway_winner_after_local_proposals_commit: false,
        pending_proposals_replay_start: 0,
        pending_proposals: Vec::new(),
        seeded_events: VecDeque::new(),
        gateway_updates: Vec::new(),
        saw_existing_speaker: true,
    };
    dave_session_mut(&mut pending.session)?
        .set_external_sender(&material.external_sender_bytes)
        .map_err(|_| VoiceError::InvalidState("voice dave external sender invalid"))?;
    dave_session_mut(&mut pending.session)?
        .init(material.protocol_version, material.group_id, &voice.user_id)
        .map_err(|_| VoiceError::InvalidState("voice dave session init failed"))?;

    gateway
        .send_dave_mls_invalid_commit_welcome(invalid_transition_id)
        .await?;
    send_pending_join_key_package(
        gateway,
        dave_session_mut(&mut pending.session)?,
        &mut pending.pending_key_package,
    )
    .await?;
    Ok(pending)
}

pub(crate) async fn reinitialize_pending_observer_dave_join_after_invalid_proposals(
    gateway: &VoiceGatewayClient,
    voice: &VoiceContext,
    material: &ObserverDaveMaterial,
    mut recognized_user_ids: BTreeSet<String>,
) -> Result<PendingObserverDaveState, VoiceError> {
    recognized_user_ids.insert(voice.user_id.clone());
    let mut pending = PendingObserverDaveState {
        session: Some(
            DaveSession::new(None)
                .map_err(|_| VoiceError::InvalidState("voice dave session create failed"))?,
        ),
        user_id: voice.user_id.clone(),
        group_id: material.group_id,
        protocol_version: material.protocol_version,
        external_sender_bytes: Some(material.external_sender_bytes.clone()),
        recognized_user_ids,
        pending_prepared_transitions: BTreeMap::new(),
        invalidated_transition_ids: BTreeSet::new(),
        pending_key_package: false,
        pending_gateway_winner_after_local_proposals_commit: false,
        pending_proposals_replay_start: 0,
        pending_proposals: Vec::new(),
        seeded_events: VecDeque::new(),
        gateway_updates: Vec::new(),
        saw_existing_speaker: true,
    };
    dave_session_mut(&mut pending.session)?
        .set_external_sender(&material.external_sender_bytes)
        .map_err(|_| VoiceError::InvalidState("voice dave external sender invalid"))?;
    dave_session_mut(&mut pending.session)?
        .init(material.protocol_version, material.group_id, &voice.user_id)
        .map_err(|_| VoiceError::InvalidState("voice dave session init failed"))?;

    tracing::debug!(
        protocol_version = material.protocol_version,
        group_id = material.group_id,
        recognized_user_ids = pending.recognized_user_ids.len(),
        "voice observer dave handshake reinitializing after invalid proposals"
    );
    gateway
        .send_dave_mls_invalid_commit_welcome(DAVE_PROTOCOL_INIT_TRANSITION_ID)
        .await?;
    send_pending_join_key_package(
        gateway,
        dave_session_mut(&mut pending.session)?,
        &mut pending.pending_key_package,
    )
    .await?;
    Ok(pending)
}

async fn restart_pending_observer_dave_join_after_invalid_transition(
    gateway: &VoiceGatewayClient,
    pending: &mut PendingObserverDaveState,
    invalid_transition_id: u16,
) -> Result<(), VoiceError> {
    let external_sender_bytes =
        pending
            .external_sender_bytes
            .clone()
            .ok_or(VoiceError::InvalidState(
                "voice dave external sender unavailable",
            ))?;
    let mut session = DaveSession::new(None)
        .map_err(|_| VoiceError::InvalidState("voice dave session create failed"))?;
    session
        .set_external_sender(&external_sender_bytes)
        .map_err(|_| VoiceError::InvalidState("voice dave external sender invalid"))?;
    session
        .init(pending.protocol_version, pending.group_id, &pending.user_id)
        .map_err(|_| VoiceError::InvalidState("voice dave session init failed"))?;

    gateway
        .send_dave_mls_invalid_commit_welcome(invalid_transition_id)
        .await?;

    pending.session = Some(session);
    pending.pending_prepared_transitions.clear();
    pending
        .invalidated_transition_ids
        .insert(invalid_transition_id);
    pending.pending_key_package = false;
    pending.pending_gateway_winner_after_local_proposals_commit = false;
    pending.pending_proposals_replay_start = 0;
    pending.pending_proposals.clear();
    pending.saw_existing_speaker = true;

    send_pending_join_key_package(
        gateway,
        dave_session_mut(&mut pending.session)?,
        &mut pending.pending_key_package,
    )
    .await
}

async fn complete_initial_proposals_transition(
    gateway: &VoiceGatewayClient,
    session: &mut Option<DaveSession>,
    local_external_sender: &DaveExternalSender,
    operation: crate::dave::DaveMlsProposalsOperation,
    proposals: &[u8],
    recognized_user_ids: &BTreeSet<String>,
) -> Result<Option<(DaveRuntimeContext, Vec<u8>)>, VoiceError> {
    let commit_welcome =
        process_initial_proposals(session, operation, proposals, recognized_user_ids)?;
    let commit_welcome = match commit_welcome {
        Some(commit_welcome) => commit_welcome,
        None => return Ok(None),
    };
    let (commit, _welcome) = local_external_sender
        .split_commit_welcome(&commit_welcome)
        .map_err(|_| VoiceError::InvalidState("voice dave commit welcome invalid"))?;
    let session = take_dave_session(session)?;
    let runtime = complete_local_initial_creator_transition(
        gateway,
        session,
        &commit_welcome,
        &commit,
        false,
    )
    .await?;
    Ok(Some((runtime, commit)))
}

fn stage_pending_observer_proposals(
    runtime: &mut DaveRuntimeContext,
    pending: &PendingObserverDaveState,
    replay_start: usize,
) -> Result<(), VoiceError> {
    let recognized = pending
        .recognized_user_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    for (operation, proposals) in pending.pending_proposals.iter().skip(replay_start) {
        let commit_welcome = runtime
            .process_proposals_with_operation(*operation, proposals, &recognized)
            .map_err(|_| VoiceError::InvalidState("voice observer dave proposals invalid"))?;
        tracing::debug!(
            proposals_len = proposals.len(),
            recognized_user_ids = recognized.len(),
            operation = ?ProposalsOperationType::from(*operation),
            produced_commit_welcome = commit_welcome.is_some(),
            "voice observer dave handshake staged buffered proposals after welcome"
        );
    }
    Ok(())
}

fn process_initial_proposals(
    session: &mut Option<DaveSession>,
    operation: crate::dave::DaveMlsProposalsOperation,
    proposals: &[u8],
    recognized_user_ids: &BTreeSet<String>,
) -> Result<Option<Vec<u8>>, VoiceError> {
    let recognized = recognized_user_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let commit_welcome = dave_session_mut(session)?
        .process_proposals_with_operation(operation, proposals, &recognized)
        .map_err(|_| VoiceError::InvalidState("voice dave proposals invalid"))?;
    match commit_welcome {
        Some(commit_welcome) => Ok(Some(commit_welcome)),
        None if ProposalsOperationType::from(operation) == ProposalsOperationType::REVOKE => {
            Ok(None)
        }
        None => Err(VoiceError::InvalidState(
            "voice dave append proposals produced no commit",
        )),
    }
}

fn observer_dave_protocol_version(pending: &PendingObserverDaveState) -> Result<u16, VoiceError> {
    pending
        .session
        .as_ref()
        .map(DaveSession::protocol_version)
        .filter(|protocol_version| *protocol_version != 0)
        .ok_or(VoiceError::InvalidState(
            "voice dave session already moved to runtime",
        ))
}

fn observer_dave_material(
    pending: &PendingObserverDaveState,
) -> Result<ObserverDaveMaterial, VoiceError> {
    Ok(ObserverDaveMaterial {
        group_id: pending.group_id,
        protocol_version: pending.protocol_version,
        external_sender_bytes: pending.external_sender_bytes.clone().ok_or(
            VoiceError::InvalidState("voice dave external sender unavailable"),
        )?,
    })
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
    send_transition_ready: bool,
) -> Result<DaveRuntimeContext, VoiceError> {
    send_local_initial_creator_commit_welcome(gateway, commit_welcome).await?;
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
    if send_transition_ready {
        tracing::debug!(
            transition_id = DAVE_PROTOCOL_INIT_TRANSITION_ID,
            commit_len = commit.len(),
            "voice dave handshake sending transition-ready after local init creator commit"
        );
        gateway
            .send_dave_transition_ready(DAVE_PROTOCOL_INIT_TRANSITION_ID)
            .await?;
    }
    Ok(runtime)
}

async fn send_local_initial_creator_commit_welcome(
    gateway: &VoiceGatewayClient,
    commit_welcome: &[u8],
) -> Result<(), VoiceError> {
    tracing::debug!(
        commit_welcome_len = commit_welcome.len(),
        "voice dave handshake sending commit welcome"
    );
    gateway
        .send_binary(protocol::dave_mls_commit_welcome_payload(commit_welcome))
        .await
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

fn dave_handshake_event_transition_id(event: &VoiceGatewayEvent) -> Option<u16> {
    match event {
        VoiceGatewayEvent::DavePrepareTransition(transition) => Some(transition.transition_id),
        VoiceGatewayEvent::DaveExecuteTransition(transition) => Some(transition.transition_id),
        VoiceGatewayEvent::DavePrepareEpoch(transition) => transition.transition_id,
        VoiceGatewayEvent::DaveMlsPrepareCommitTransition(transition) => {
            Some(transition.transition_id)
        }
        VoiceGatewayEvent::DaveMlsWelcome(transition) => Some(transition.transition_id),
        _ => None,
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
            Some("wss://voice.example.discord.gg/?v=8".to_owned())
        );
    }

    #[test]
    fn gateway_url_keeps_loopback_endpoints_on_ws() {
        assert_eq!(
            gateway_url("127.0.0.1:9000").unwrap(),
            Some("ws://127.0.0.1:9000/?v=8".to_owned())
        );
    }

    #[test]
    fn gateway_url_preserves_existing_query_params() {
        assert_eq!(
            gateway_url("wss://voice.example.discord.gg/?encoding=json").unwrap(),
            Some("wss://voice.example.discord.gg/?encoding=json&v=8".to_owned())
        );
    }

    #[test]
    fn gateway_url_does_not_duplicate_explicit_version() {
        assert_eq!(
            gateway_url("wss://voice.example.discord.gg/?v=8&encoding=json").unwrap(),
            Some("wss://voice.example.discord.gg/?v=8&encoding=json".to_owned())
        );
    }
}
