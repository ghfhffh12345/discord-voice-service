use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::{Duration, timeout};

use crate::dave::{DaveExternalSender, DaveRuntimeContext};
use crate::error::VoiceError;
use crate::gateway::VoiceGatewayClient;
use crate::handshake;
use crate::protection::ProtectionContext;
use crate::protocol::{
    self, ClientDisconnect, ClientsConnect, DaveExecuteTransition, DaveMlsPrepareCommitTransition,
    DaveMlsProposals, DaveMlsWelcome, DavePrepareEpoch, SessionDescription, VoiceGatewayEvent,
};
use crate::rollover::VoiceSessionRollover;
use crate::speaking::{OPUS_SILENCE_FRAME, send_speaking};
use crate::udp::VoiceUdpTransport;

const DAVE_GATEWAY_EVENT_DRAIN_LIMIT: usize = 256;
const DAVE_GATEWAY_IDLE_POLL: Duration = Duration::from_millis(1);
const DAVE_GATEWAY_TRANSITION_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceContext {
    pub guild_id: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_id: String,
    pub endpoint: String,
    pub token: String,
}

pub struct ConnectedVoiceSession {
    voice: VoiceContext,
    rollover: VoiceSessionRollover,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
    ssrc: Option<u32>,
    session_description: Option<SessionDescription>,
    dave: Option<DaveRuntimeContext>,
    dave_group_id: Option<u64>,
    dave_external_sender: Option<DaveExternalSender>,
    dave_recognized_user_ids: BTreeSet<String>,
    completed_dave_transition_ids: BTreeSet<u16>,
    pending_dave_prepared_transitions: BTreeMap<u16, u16>,
    pending_dave_local_commit_transition: Option<DaveRuntimeContext>,
    pending_dave_transition: Option<(u16, DaveRuntimeContext)>,
    dave_failed_closed: bool,
    heartbeat_shutdown: Option<oneshot::Sender<()>>,
    speaking_started: bool,
}

impl ConnectedVoiceSession {
    pub(crate) fn new(voice: VoiceContext) -> Self {
        let user_id = voice.user_id.clone();
        Self {
            voice,
            rollover: VoiceSessionRollover::default(),
            gateway: None,
            transport: None,
            ssrc: None,
            session_description: None,
            dave: None,
            dave_group_id: None,
            dave_external_sender: None,
            dave_recognized_user_ids: BTreeSet::from([user_id]),
            completed_dave_transition_ids: BTreeSet::new(),
            pending_dave_prepared_transitions: BTreeMap::new(),
            pending_dave_local_commit_transition: None,
            pending_dave_transition: None,
            dave_failed_closed: false,
            heartbeat_shutdown: None,
            speaking_started: false,
        }
    }

    pub async fn connect(voice: VoiceContext) -> Result<Self, VoiceError> {
        let Some(result) = handshake::connect(&voice).await? else {
            return Ok(Self::new(voice));
        };
        let handshake::VoiceHandshakeResult {
            gateway,
            transport,
            ssrc,
            heartbeat_shutdown,
            session_description,
            dave,
        } = result;
        let transport =
            transport.with_protection(ProtectionContext::from_session(&session_description)?);
        let (
            dave,
            dave_group_id,
            dave_external_sender,
            dave_recognized_user_ids,
            completed_dave_transition_ids,
        ) =
            if let Some(dave) = dave {
                (
                    Some(dave.runtime),
                    Some(dave.group_id),
                    Some(dave.external_sender),
                    dave.recognized_user_ids,
                    dave.completed_transition_ids,
                )
            } else {
                (
                    None,
                    None,
                    None,
                    BTreeSet::from([voice.user_id.clone()]),
                    BTreeSet::new(),
                )
            };

        Ok(Self {
            gateway: Some(gateway),
            transport: Some(transport),
            voice,
            rollover: VoiceSessionRollover::default(),
            ssrc: Some(ssrc),
            session_description: Some(session_description),
            dave,
            dave_group_id,
            dave_external_sender,
            dave_recognized_user_ids,
            completed_dave_transition_ids,
            pending_dave_prepared_transitions: BTreeMap::new(),
            pending_dave_local_commit_transition: None,
            pending_dave_transition: None,
            dave_failed_closed: false,
            heartbeat_shutdown: Some(heartbeat_shutdown),
            speaking_started: false,
        })
    }

    pub fn voice_context(&self) -> &VoiceContext {
        &self.voice
    }

    pub fn is_connected(&self) -> bool {
        self.gateway.is_some()
            && self.transport.is_some()
            && self.ssrc.is_some()
            && self.session_description.is_some()
    }

    pub fn dave_enabled(&self) -> bool {
        self.dave.is_some()
            || self.pending_dave_transition.is_some()
            || self.pending_dave_local_commit_transition.is_some()
            || !self.pending_dave_prepared_transitions.is_empty()
            || self.dave_failed_closed
    }

    pub fn media_started(&self) -> bool {
        self.speaking_started
    }

    pub fn recovering(&self) -> bool {
        self.rollover.recovering()
    }

    pub fn voice_reconnecting(&self) -> bool {
        self.rollover.voice_reconnecting()
    }

    pub async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), VoiceError> {
        self.process_pending_gateway_events().await?;
        let ssrc = self
            .ssrc
            .ok_or(VoiceError::InvalidState("voice ssrc unavailable"))?;
        if !self.speaking_started {
            let gateway = self
                .gateway
                .as_ref()
                .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;
            send_speaking(gateway, ssrc).await?;
            self.speaking_started = true;
        }

        let frame = if let Some(dave) = self.dave.as_mut() {
            Bytes::from(
                dave.encrypt_audio_frame(frame.as_ref())
                    .map_err(|_| VoiceError::InvalidState("voice dave frame encryption failed"))?,
            )
        } else {
            frame
        };
        let transport = self
            .transport
            .as_mut()
            .ok_or(VoiceError::InvalidState("voice transport unavailable"))?;
        transport.send_audio_frame(frame).await?;
        self.speaking_started = true;
        Ok(())
    }

    async fn process_pending_gateway_events(&mut self) -> Result<(), VoiceError> {
        self.process_pending_gateway_events_with_initial_poll(DAVE_GATEWAY_IDLE_POLL)
            .await
    }

    pub async fn wait_for_initial_dave_settle(&mut self) -> Result<(), VoiceError> {
        self.process_pending_gateway_events_with_initial_poll(DAVE_GATEWAY_TRANSITION_POLL)
            .await
    }

    async fn process_pending_gateway_events_with_initial_poll(
        &mut self,
        initial_wait: Duration,
    ) -> Result<(), VoiceError> {
        if self.dave_failed_closed {
            return Err(VoiceError::InvalidState("voice dave session failed closed"));
        }
        if self.dave.is_none()
            && self.pending_dave_transition.is_none()
            && self.pending_dave_local_commit_transition.is_none()
            && self.pending_dave_prepared_transitions.is_empty()
        {
            return Ok(());
        }
        let gateway = self
            .gateway
            .clone()
            .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;

        let mut drained_any_event = false;
        let mut reached_drain_limit = true;
        for _ in 0..DAVE_GATEWAY_EVENT_DRAIN_LIMIT {
            let wait = if self.pending_dave_transition.is_some()
                || self.pending_dave_local_commit_transition.is_some()
                || !self.pending_dave_prepared_transitions.is_empty()
                || drained_any_event
            {
                DAVE_GATEWAY_TRANSITION_POLL
            } else {
                initial_wait
            };
            let payload = match timeout(wait, gateway.receive_event()).await {
                Ok(Ok(payload)) => payload,
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    reached_drain_limit = false;
                    break;
                }
            };
            drained_any_event = true;
            let event = payload.into_event();
            tracing::debug!(
                event = dave_session_event_name(&event),
                pending_prepared_transitions = self.pending_dave_prepared_transitions.len(),
                has_pending_transition = self.pending_dave_transition.is_some(),
                has_pending_local_commit_transition =
                    self.pending_dave_local_commit_transition.is_some(),
                recognized_user_ids = self.dave_recognized_user_ids.len(),
                "voice dave session draining gateway event"
            );
            if let Err(err) = self.process_gateway_event_for_dave(&gateway, event).await {
                return Err(self.fail_dave_closed(err));
            }
        }

        if reached_drain_limit {
            return Err(VoiceError::InvalidState(
                "voice dave gateway event backlog pending",
            ));
        }
        if self.pending_dave_transition.is_some() {
            return Err(VoiceError::InvalidState("voice dave transition pending"));
        }
        if self.pending_dave_local_commit_transition.is_some() {
            return Err(VoiceError::InvalidState(
                "voice dave local commit transition pending",
            ));
        }
        if !self.pending_dave_prepared_transitions.is_empty() {
            return Err(VoiceError::InvalidState(
                "voice dave prepared transition pending",
            ));
        }
        Ok(())
    }

    async fn process_gateway_event_for_dave(
        &mut self,
        gateway: &VoiceGatewayClient,
        event: VoiceGatewayEvent,
    ) -> Result<(), VoiceError> {
        match event {
            VoiceGatewayEvent::ClientsConnect(ClientsConnect { user_ids }) => {
                let user_count = user_ids.len();
                self.dave_recognized_user_ids.extend(user_ids);
                tracing::debug!(
                    user_count,
                    recognized_user_ids = self.dave_recognized_user_ids.len(),
                    "voice dave session updated recognized users"
                );
            }
            VoiceGatewayEvent::ClientDisconnect(ClientDisconnect { user_id }) => {
                self.dave_recognized_user_ids.remove(&user_id);
                tracing::debug!(
                    %user_id,
                    recognized_user_ids = self.dave_recognized_user_ids.len(),
                    "voice dave session removed disconnected user"
                );
            }
            VoiceGatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
                transition_id,
                epoch,
                protocol_version,
            }) => {
                if epoch.is_empty() {
                    return Err(VoiceError::InvalidState("voice dave prepare epoch missing"));
                }
                let current_protocol = self.current_dave_protocol_version()?;
                if protocol_version != current_protocol {
                    return Err(VoiceError::InvalidState(
                        "voice dave prepare epoch protocol version mismatch",
                    ));
                }
                if let Some(runtime) = self.pending_dave_local_commit_transition.take() {
                    tracing::debug!(
                        transition_id,
                        group_id = ?self.dave_group_id,
                        epoch = %epoch,
                        protocol_version,
                        "voice dave session prepared deferred local transition epoch"
                    );
                    self.ready_pending_transition(gateway, transition_id, runtime)
                        .await?;
                } else {
                    self.pending_dave_prepared_transitions
                        .insert(transition_id, protocol_version);
                    tracing::debug!(
                        transition_id,
                        group_id = ?self.dave_group_id,
                        epoch = %epoch,
                        protocol_version,
                        "voice dave session prepared transition epoch"
                    );
                }
            }
            VoiceGatewayEvent::DaveMlsPrepareCommitTransition(DaveMlsPrepareCommitTransition {
                transition_id,
                commit,
            }) => {
                self.prepare_remote_commit_transition(gateway, transition_id, &commit)
                    .await?;
            }
            VoiceGatewayEvent::DaveMlsWelcome(DaveMlsWelcome {
                transition_id,
                welcome,
            }) => {
                self.prepare_welcome_transition(gateway, transition_id, &welcome)
                    .await?;
            }
            VoiceGatewayEvent::DaveMlsProposals(DaveMlsProposals {
                operation,
                proposals,
            }) => {
                self.prepare_local_commit_transition(gateway, operation, &proposals)
                    .await?;
            }
            VoiceGatewayEvent::DaveExecuteTransition(DaveExecuteTransition { transition_id }) => {
                match self.pending_dave_transition.take() {
                    Some((expected_transition_id, runtime))
                        if expected_transition_id == transition_id =>
                    {
                        self.completed_dave_transition_ids.insert(transition_id);
                        self.dave = Some(runtime);
                        tracing::debug!(
                            transition_id,
                            "voice dave session executed pending transition"
                        );
                    }
                    Some((expected_transition_id, runtime)) => {
                        self.pending_dave_transition = Some((expected_transition_id, runtime));
                        return Err(VoiceError::InvalidState(
                            "voice dave execute transition id mismatch",
                        ));
                    }
                    None => {
                        tracing::debug!(
                            transition_id,
                            "voice dave session ignored execute without pending transition"
                        );
                    }
                }
            }
            VoiceGatewayEvent::DavePrepareTransition(_) => {
                return Err(VoiceError::InvalidState(
                    "voice dave protocol downgrade transition unsupported",
                ));
            }
            _ => {}
        }
        Ok(())
    }

    async fn prepare_remote_commit_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        transition_id: u16,
        commit: &[u8],
    ) -> Result<(), VoiceError> {
        if self.completed_dave_transition_ids.contains(&transition_id)
            && self.pending_dave_prepared_transitions.is_empty()
        {
            tracing::debug!(
                transition_id,
                "voice dave session ignoring duplicate commit transition already applied via welcome"
            );
            return Ok(());
        }
        match self.consume_prepared_transition(transition_id) {
            Ok(_) => {}
            Err(VoiceError::InvalidState("voice dave transition missing prepared epoch"))
                if self.pending_dave_prepared_transitions.is_empty() =>
            {
                tracing::debug!(
                    transition_id,
                    "voice dave session accepting prepare commit transition without cached prepare epoch"
                );
            }
            Err(err) => return Err(err),
        }
        let mut runtime = self
            .dave
            .take()
            .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))?;
        let commit_result = runtime
            .process_commit(commit)
            .map_err(|_| VoiceError::InvalidState("voice dave commit invalid"))?;
        if commit_result.is_ignored() {
            self.dave = Some(runtime);
            return Ok(());
        }
        if commit_result.is_failed() || commit_result.roster_member_ids().is_empty() {
            return Err(VoiceError::InvalidState(
                "voice dave commit transition did not keep group",
            ));
        }
        self.ready_pending_transition(gateway, transition_id, runtime)
            .await
    }

    async fn prepare_welcome_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        transition_id: u16,
        welcome: &[u8],
    ) -> Result<(), VoiceError> {
        self.consume_prepared_transition(transition_id)?;
        let mut runtime = self
            .dave
            .take()
            .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))?;
        let recognized = self
            .dave_recognized_user_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let welcome_result = runtime
            .process_welcome(welcome, &recognized)
            .map_err(|_| VoiceError::InvalidState("voice dave welcome invalid"))?;
        if welcome_result.roster_member_ids().is_empty() {
            return Err(VoiceError::InvalidState(
                "voice dave welcome transition did not keep group",
            ));
        }
        self.ready_pending_transition(gateway, transition_id, runtime)
            .await
    }

    async fn prepare_local_commit_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        operation: crate::dave::DaveMlsProposalsOperation,
        proposals: &[u8],
    ) -> Result<(), VoiceError> {
        let transition_id = self.next_prepared_transition_id().ok();
        let mut runtime = self
            .dave
            .take()
            .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))?;
        let recognized = self
            .dave_recognized_user_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let Some(commit_welcome) = runtime
            .process_proposals_with_operation(operation, proposals, &recognized)
            .map_err(|_| VoiceError::InvalidState("voice dave proposals invalid"))?
        else {
            self.dave = Some(runtime);
            return Ok(());
        };
        let external_sender =
            self.dave_external_sender
                .as_ref()
                .ok_or(VoiceError::InvalidState(
                    "voice dave external sender unavailable",
                ))?;
        let (commit, _welcome) = external_sender
            .split_commit_welcome(&commit_welcome)
            .map_err(|_| VoiceError::InvalidState("voice dave commit welcome invalid"))?;
        gateway
            .send_binary(protocol::dave_mls_commit_welcome_payload(&commit_welcome))
            .await?;
        let commit_result = runtime
            .process_commit(&commit)
            .map_err(|_| VoiceError::InvalidState("voice dave local commit invalid"))?;
        if commit_result.is_failed()
            || commit_result.is_ignored()
            || commit_result.roster_member_ids().is_empty()
        {
            return Err(VoiceError::InvalidState(
                "voice dave local commit transition did not keep group",
            ));
        }
        if let Some(transition_id) = transition_id {
            self.consume_prepared_transition(transition_id)?;
            self.ready_pending_transition(gateway, transition_id, runtime)
                .await
        } else {
            self.pending_dave_local_commit_transition = Some(runtime);
            Ok(())
        }
    }

    async fn ready_pending_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        transition_id: u16,
        runtime: DaveRuntimeContext,
    ) -> Result<(), VoiceError> {
        self.pending_dave_transition = Some((transition_id, runtime));
        if let Err(err) = gateway.send_dave_transition_ready(transition_id).await {
            return Err(self.fail_dave_closed(err));
        }
        Ok(())
    }

    fn fail_dave_closed(&mut self, err: VoiceError) -> VoiceError {
        tracing::debug!(
            error = ?err,
            "voice dave session failed closed after gateway transition error"
        );
        self.dave = None;
        self.pending_dave_transition = None;
        self.pending_dave_local_commit_transition = None;
        self.pending_dave_prepared_transitions.clear();
        self.dave_failed_closed = true;
        err
    }

    fn consume_prepared_transition(&mut self, transition_id: u16) -> Result<u16, VoiceError> {
        self.pending_dave_prepared_transitions
            .remove(&transition_id)
            .ok_or(VoiceError::InvalidState(
                "voice dave transition missing prepared epoch",
            ))
    }

    fn next_prepared_transition_id(&self) -> Result<u16, VoiceError> {
        self.pending_dave_prepared_transitions
            .keys()
            .next()
            .copied()
            .ok_or(VoiceError::InvalidState(
                "voice dave proposals missing prepared epoch",
            ))
    }

    fn current_dave_protocol_version(&self) -> Result<u16, VoiceError> {
        self.dave
            .as_ref()
            .map(|runtime| runtime.protocol_version)
            .or_else(|| {
                self.pending_dave_transition
                    .as_ref()
                    .map(|(_, runtime)| runtime.protocol_version)
            })
            .or_else(|| {
                self.pending_dave_local_commit_transition
                    .as_ref()
                    .map(|runtime| runtime.protocol_version)
            })
            .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))
    }

    pub async fn stop_audio(&mut self) -> Result<(), VoiceError> {
        for silence_frame_index in 0..5 {
            if let Err(err) = self
                .send_audio_frame(Bytes::copy_from_slice(&OPUS_SILENCE_FRAME))
                .await
            {
                tracing::debug!(
                    silence_frame_index,
                    error = ?err,
                    "voice stop_audio silence send failed"
                );
                return Err(err);
            }
        }
        self.speaking_started = false;
        Ok(())
    }
}

fn dave_session_event_name(event: &VoiceGatewayEvent) -> &'static str {
    match event {
        VoiceGatewayEvent::ClientsConnect(_) => "clients_connect",
        VoiceGatewayEvent::ClientDisconnect(_) => "client_disconnect",
        VoiceGatewayEvent::DavePrepareEpoch(_) => "dave_prepare_epoch",
        VoiceGatewayEvent::DaveMlsPrepareCommitTransition(_) => "dave_mls_prepare_commit_transition",
        VoiceGatewayEvent::DaveMlsWelcome(_) => "dave_mls_welcome",
        VoiceGatewayEvent::DaveMlsProposals(_) => "dave_mls_proposals",
        VoiceGatewayEvent::DaveExecuteTransition(_) => "dave_execute_transition",
        VoiceGatewayEvent::Speaking(_) => "speaking",
        VoiceGatewayEvent::HeartbeatAck(_) => "heartbeat_ack",
        VoiceGatewayEvent::SessionDescription(_) => "session_description",
        VoiceGatewayEvent::MediaSinkWants => "media_sink_wants",
        VoiceGatewayEvent::ClientFlags(_) => "client_flags",
        VoiceGatewayEvent::ClientPlatform(_) => "client_platform",
        VoiceGatewayEvent::DavePrepareTransition(_) => "dave_prepare_transition",
        VoiceGatewayEvent::Video(_) => "video",
        VoiceGatewayEvent::Ready(_) => "ready",
        VoiceGatewayEvent::Resumed => "resumed",
        VoiceGatewayEvent::Hello(_) => "hello",
        VoiceGatewayEvent::DaveMlsExternalSenderPackage(_) => "dave_mls_external_sender_package",
    }
}

impl Drop for ConnectedVoiceSession {
    fn drop(&mut self) {
        if let Some(shutdown) = self.heartbeat_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use futures::StreamExt;
    use serde_json::Value;
    use tokio::net::{TcpListener, UdpSocket};
    use tokio::sync::{Mutex, Notify};
    use tokio::time::{Duration, Instant, sleep};
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    struct FakeUdpPeer {
        addr: SocketAddr,
        silence_frame_count: Arc<Mutex<usize>>,
    }

    impl FakeUdpPeer {
        async fn spawn() -> Self {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            let silence_frame_count = Arc::new(Mutex::new(0usize));
            let silence_frame_count_state = Arc::clone(&silence_frame_count);

            tokio::spawn(async move {
                let mut buf = [0u8; 512];
                loop {
                    let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                        break;
                    };
                    let packet = &buf[..len];

                    if packet.len() == 74 && packet[..2] == 1u16.to_be_bytes() {
                        let mut response = [0u8; 74];
                        response[..2].copy_from_slice(&2u16.to_be_bytes());
                        response[2..4].copy_from_slice(&70u16.to_be_bytes());
                        response[4..8].copy_from_slice(&packet[4..8]);
                        response[8..17].copy_from_slice(b"127.0.0.1");
                        response[72..74].copy_from_slice(&from.port().to_be_bytes());
                        socket.send_to(&response, from).await.unwrap();
                        continue;
                    }

                    if packet.ends_with(&OPUS_SILENCE_FRAME) {
                        *silence_frame_count_state.lock().await += 1;
                    }
                }
            });

            Self {
                addr,
                silence_frame_count,
            }
        }

        fn addr(&self) -> SocketAddr {
            self.addr
        }

        async fn silence_frame_count(&self) -> usize {
            wait_for_value(&self.silence_frame_count, |count| *count >= 5).await
        }
    }

    struct FakeVoiceGateway {
        url: String,
        speaking_observed: Arc<Notify>,
    }

    impl FakeVoiceGateway {
        async fn spawn() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws_addr = listener.local_addr().unwrap();
            let speaking_observed = Arc::new(Notify::new());
            let speaking_observed_state = Arc::clone(&speaking_observed);

            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();

                while let Some(message) = ws.next().await {
                    let message = message.unwrap();
                    if let Message::Text(text) = message {
                        let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                        if payload.get("op").and_then(Value::as_u64) == Some(5) {
                            speaking_observed_state.notify_waiters();
                        }
                    }
                }
            });

            Self {
                url: format!("ws://{ws_addr}/"),
                speaking_observed,
            }
        }

        fn url(&self) -> &str {
            &self.url
        }

        fn speaking_observed(&self) -> Arc<Notify> {
            Arc::clone(&self.speaking_observed)
        }
    }

    #[tokio::test]
    async fn stop_audio_emits_speaking_and_silence_frames() {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let speaking_observed = gateway.speaking_observed();
        let speaking_notified = speaking_observed.notified();

        session.stop_audio().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), speaking_notified)
            .await
            .expect("speaking should be observed");
        assert_eq!(udp.silence_frame_count().await, 5);
    }

    async fn test_connected_session(url: &str, udp_addr: SocketAddr) -> ConnectedVoiceSession {
        ConnectedVoiceSession {
            voice: VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                user_id: "user-1".into(),
                session_id: "session-1".into(),
                endpoint: url.into(),
                token: "token-1".into(),
            },
            rollover: VoiceSessionRollover::default(),
            gateway: Some(VoiceGatewayClient::connect(url).await.unwrap()),
            transport: Some(VoiceUdpTransport::connect(udp_addr, 7).await.unwrap()),
            ssrc: Some(7),
            session_description: Some(SessionDescription {
                mode: "xsalsa20_poly1305".into(),
                secret_key: vec![0; 32],
                dave_protocol_version: None,
            }),
            dave: None,
            dave_group_id: None,
            dave_external_sender: None,
            dave_recognized_user_ids: BTreeSet::from(["user-1".to_owned()]),
            completed_dave_transition_ids: BTreeSet::new(),
            pending_dave_prepared_transitions: BTreeMap::new(),
            pending_dave_local_commit_transition: None,
            pending_dave_transition: None,
            dave_failed_closed: false,
            heartbeat_shutdown: None,
            speaking_started: false,
        }
    }

    async fn wait_for_value<T, F>(slot: &Arc<Mutex<T>>, ready: F) -> T
    where
        T: Clone,
        F: Fn(&T) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let value = slot.lock().await.clone();
            if ready(&value) || Instant::now() >= deadline {
                return value;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}
