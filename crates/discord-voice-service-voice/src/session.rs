use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::{Duration, timeout};

use crate::dave::{DaveExternalSender, DaveRuntimeContext, DaveSession};
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
const DAVE_PROTOCOL_INIT_TRANSITION_ID: u16 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingDaveTransitionSource {
    CommitBacked,
    WelcomeBacked,
}

struct PendingDaveTransition {
    transition_id: u16,
    runtime: DaveRuntimeContext,
    source: PendingDaveTransitionSource,
}

struct PendingInitialDaveSession {
    session: DaveSession,
    protocol_version: u16,
}

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
    pending_initial_dave: Option<PendingInitialDaveSession>,
    dave_group_id: Option<u64>,
    dave_external_sender: Option<DaveExternalSender>,
    dave_external_sender_bytes: Option<Vec<u8>>,
    dave_recognized_user_ids: BTreeSet<String>,
    completed_welcome_backed_dave_transition_ids: BTreeSet<u16>,
    completed_local_init_commit_transition_ids: BTreeSet<u16>,
    pending_dave_prepared_transitions: BTreeMap<u16, u16>,
    pending_dave_local_init_commit_echoes: BTreeMap<u16, Vec<u8>>,
    pending_dave_local_commit_transition: Option<DaveRuntimeContext>,
    pending_dave_transition: Option<PendingDaveTransition>,
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
            pending_initial_dave: None,
            dave_group_id: None,
            dave_external_sender: None,
            dave_external_sender_bytes: None,
            dave_recognized_user_ids: BTreeSet::from([user_id]),
            completed_welcome_backed_dave_transition_ids: BTreeSet::new(),
            completed_local_init_commit_transition_ids: BTreeSet::new(),
            pending_dave_prepared_transitions: BTreeMap::new(),
            pending_dave_local_init_commit_echoes: BTreeMap::new(),
            pending_dave_local_commit_transition: None,
            pending_dave_transition: None,
            dave_failed_closed: false,
            heartbeat_shutdown: None,
            speaking_started: false,
        }
    }

    pub async fn connect(voice: VoiceContext) -> Result<Self, VoiceError> {
        let Some(result) = handshake::connect_active_participant(&voice).await? else {
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
            pending_initial_dave,
            dave_group_id,
            dave_external_sender,
            dave_external_sender_bytes,
            dave_recognized_user_ids,
            completed_welcome_backed_dave_transition_ids,
            completed_local_init_commit_transition_ids,
        ) = if let Some(dave) = dave {
            let pending_initial_dave =
                dave.pending_initial_session
                    .map(|session| PendingInitialDaveSession {
                        session,
                        protocol_version: dave.protocol_version,
                    });
            (
                dave.runtime,
                pending_initial_dave,
                Some(dave.group_id),
                Some(dave.external_sender),
                Some(dave.external_sender_bytes),
                dave.recognized_user_ids,
                dave.completed_welcome_backed_transition_ids,
                dave.completed_local_init_commit_transition_ids,
            )
        } else {
            (
                None,
                None,
                None,
                None,
                None,
                BTreeSet::from([voice.user_id.clone()]),
                BTreeSet::new(),
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
            pending_initial_dave,
            dave_group_id,
            dave_external_sender,
            dave_external_sender_bytes,
            dave_recognized_user_ids,
            completed_welcome_backed_dave_transition_ids,
            completed_local_init_commit_transition_ids,
            pending_dave_prepared_transitions: BTreeMap::new(),
            pending_dave_local_init_commit_echoes: BTreeMap::new(),
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
            || self.pending_initial_dave.is_some()
            || self.pending_dave_transition.is_some()
            || self.pending_dave_local_commit_transition.is_some()
            || !self.pending_dave_prepared_transitions.is_empty()
            || !self.pending_dave_local_init_commit_echoes.is_empty()
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

        let frame = if frame.as_ref() == OPUS_SILENCE_FRAME.as_slice() {
            frame
        } else if let Some(dave) = self.dave.as_mut() {
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
            && self.pending_initial_dave.is_none()
            && self.pending_dave_transition.is_none()
            && self.pending_dave_local_commit_transition.is_none()
            && self.pending_dave_prepared_transitions.is_empty()
            && self.pending_dave_local_init_commit_echoes.is_empty()
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
                || !self.pending_dave_local_init_commit_echoes.is_empty()
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
                pending_local_init_commit_echoes = self.pending_dave_local_init_commit_echoes.len(),
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
                let transition_id = transition_id.ok_or(VoiceError::InvalidState(
                    "voice dave prepare epoch transition id missing",
                ))?;
                if self
                    .completed_local_init_commit_transition_ids
                    .contains(&transition_id)
                {
                    tracing::debug!(
                        transition_id,
                        group_id = ?self.dave_group_id,
                        epoch = %epoch,
                        protocol_version,
                        "voice dave session ignoring duplicate init prepare epoch already applied during handshake"
                    );
                    return Ok(());
                }
                if let Some(runtime) = self.pending_dave_local_commit_transition.take() {
                    tracing::debug!(
                        transition_id,
                        group_id = ?self.dave_group_id,
                        epoch = %epoch,
                        protocol_version,
                        "voice dave session prepared deferred local transition epoch"
                    );
                    self.ready_pending_transition(
                        gateway,
                        transition_id,
                        runtime,
                        PendingDaveTransitionSource::CommitBacked,
                    )
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
                    Some(PendingDaveTransition {
                        transition_id: expected_transition_id,
                        runtime,
                        source,
                    }) if expected_transition_id == transition_id => {
                        self.mark_completed_transition(transition_id, source);
                        self.dave = Some(runtime);
                        tracing::debug!(
                            transition_id,
                            "voice dave session executed pending transition"
                        );
                    }
                    Some(pending_transition) => {
                        self.pending_dave_transition = Some(pending_transition);
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
            VoiceGatewayEvent::DavePrepareTransition(transition) => {
                if transition.transition_id == DAVE_PROTOCOL_INIT_TRANSITION_ID {
                    self.pending_dave_prepared_transitions
                        .insert(transition.transition_id, transition.protocol_version);
                    tracing::debug!(
                        transition_id = transition.transition_id,
                        protocol_version = transition.protocol_version,
                        "voice dave session prepared init/reset transition"
                    );
                    return Ok(());
                }
                if transition.protocol_version == 0 {
                    return Err(VoiceError::InvalidState(
                        "voice dave protocol downgrade transition unsupported",
                    ));
                }
                self.pending_dave_prepared_transitions
                    .insert(transition.transition_id, transition.protocol_version);
                tracing::debug!(
                    transition_id = transition.transition_id,
                    protocol_version = transition.protocol_version,
                    "voice dave session prepared protocol transition"
                );
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
        if self
            .completed_welcome_backed_dave_transition_ids
            .contains(&transition_id)
            && !self
                .pending_dave_prepared_transitions
                .contains_key(&transition_id)
        {
            tracing::debug!(
                transition_id,
                "voice dave session ignoring duplicate commit transition already applied via welcome"
            );
            return Ok(());
        }
        if self
            .completed_local_init_commit_transition_ids
            .contains(&transition_id)
        {
            if let Some(expected_commit) = self
                .pending_dave_local_init_commit_echoes
                .remove(&transition_id)
            {
                if expected_commit.as_slice() != commit {
                    return Err(VoiceError::InvalidState(
                        "voice dave local init commit echo mismatch",
                    ));
                }
                self.pending_dave_prepared_transitions
                    .remove(&transition_id);
                tracing::debug!(
                    transition_id,
                    commit_len = commit.len(),
                    "voice dave session confirmed local init commit echo already applied"
                );
                return Ok(());
            }
            self.pending_dave_prepared_transitions
                .remove(&transition_id);
            tracing::debug!(
                transition_id,
                "voice dave session ignoring duplicate local init commit transition already applied during handshake"
            );
            return Ok(());
        }
        self.consume_prepared_transition(transition_id)?;
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
        self.ready_pending_transition(
            gateway,
            transition_id,
            runtime,
            PendingDaveTransitionSource::CommitBacked,
        )
        .await
    }

    async fn prepare_welcome_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        transition_id: u16,
        welcome: &[u8],
    ) -> Result<(), VoiceError> {
        if self
            .completed_welcome_backed_dave_transition_ids
            .contains(&transition_id)
            && !self
                .pending_dave_prepared_transitions
                .contains_key(&transition_id)
        {
            tracing::debug!(
                transition_id,
                "voice dave session ignoring duplicate welcome transition already applied via welcome"
            );
            return Ok(());
        }
        if self
            .completed_local_init_commit_transition_ids
            .contains(&transition_id)
        {
            self.pending_dave_prepared_transitions
                .remove(&transition_id);
            tracing::debug!(
                transition_id,
                "voice dave session ignoring duplicate local init welcome transition already applied during handshake"
            );
            return Ok(());
        }
        self.consume_prepared_transition(transition_id)?;
        let mut runtime = self
            .dave
            .take()
            .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))?;
        let recognized_user_ids = self
            .dave_recognized_user_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let recognized = recognized_user_ids
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
        self.ready_pending_transition(
            gateway,
            transition_id,
            runtime,
            PendingDaveTransitionSource::WelcomeBacked,
        )
        .await
    }

    async fn prepare_local_commit_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        operation: crate::dave::DaveMlsProposalsOperation,
        proposals: &[u8],
    ) -> Result<(), VoiceError> {
        let transition_id = self.next_prepared_transition_id().ok();
        if let Some(pending_initial) = self.pending_initial_dave.take() {
            return self
                .prepare_pending_initial_local_commit_transition(
                    gateway,
                    operation,
                    proposals,
                    pending_initial,
                    transition_id,
                )
                .await;
        }
        let mut runtime = self
            .dave
            .take()
            .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))?;
        let recognized_user_ids = self
            .dave_recognized_user_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let recognized = recognized_user_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let commit_welcome = match runtime.process_proposals_with_operation(
            operation,
            proposals,
            &recognized,
        ) {
            Ok(Some(commit_welcome)) => commit_welcome,
            Ok(None) => {
                self.dave = Some(runtime);
                return Ok(());
            }
            Err(err) if self.should_retry_initial_epoch_proposals(operation) => {
                tracing::debug!(
                    error = ?err,
                    recognized_user_ids = recognized.len(),
                    "voice dave session retrying proposals against initial epoch after self-only local init"
                );
                return self
                    .prepare_initial_epoch_local_commit_transition(
                        gateway,
                        operation,
                        proposals,
                        runtime.protocol_version,
                        &recognized,
                    )
                    .await;
            }
            Err(_) => return Err(VoiceError::InvalidState("voice dave proposals invalid")),
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
            self.ready_pending_transition(
                gateway,
                transition_id,
                runtime,
                PendingDaveTransitionSource::CommitBacked,
            )
            .await
        } else {
            self.pending_dave_local_commit_transition = Some(runtime);
            Ok(())
        }
    }

    fn should_retry_initial_epoch_proposals(
        &self,
        operation: crate::dave::DaveMlsProposalsOperation,
    ) -> bool {
        operation == crate::dave::DaveMlsProposalsOperation::Append
            && self
                .completed_local_init_commit_transition_ids
                .contains(&DAVE_PROTOCOL_INIT_TRANSITION_ID)
            && self.completed_welcome_backed_dave_transition_ids.is_empty()
    }

    async fn prepare_initial_epoch_local_commit_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        operation: crate::dave::DaveMlsProposalsOperation,
        proposals: &[u8],
        protocol_version: u16,
        recognized: &[&str],
    ) -> Result<(), VoiceError> {
        let external_sender =
            self.dave_external_sender
                .as_ref()
                .ok_or(VoiceError::InvalidState(
                    "voice dave external sender unavailable",
                ))?;
        let group_id = self
            .dave_group_id
            .ok_or(VoiceError::InvalidState("voice dave group id unavailable"))?;
        let external_sender_bytes =
            self.dave_external_sender_bytes
                .as_deref()
                .ok_or(VoiceError::InvalidState(
                    "voice dave external sender unavailable",
                ))?;
        let mut session = DaveSession::new(None)
            .map_err(|_| VoiceError::InvalidState("voice dave session create failed"))?;
        session
            .set_external_sender(external_sender_bytes)
            .map_err(|_| VoiceError::InvalidState("voice dave external sender invalid"))?;
        session
            .init(protocol_version, group_id, &self.voice.user_id)
            .map_err(|_| VoiceError::InvalidState("voice dave session init failed"))?;
        let Some(commit_welcome) = session
            .process_proposals_with_operation(operation, proposals, recognized)
            .map_err(|_| VoiceError::InvalidState("voice dave proposals invalid"))?
        else {
            return Err(VoiceError::InvalidState(
                "voice dave append proposals produced no commit",
            ));
        };
        let (commit, _welcome) = external_sender
            .split_commit_welcome(&commit_welcome)
            .map_err(|_| VoiceError::InvalidState("voice dave commit welcome invalid"))?;
        gateway
            .send_binary(protocol::dave_mls_commit_welcome_payload(&commit_welcome))
            .await?;
        let commit_result = session
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
        self.dave = Some(
            DaveRuntimeContext::from_session(session)
                .map_err(|_| VoiceError::InvalidState("voice dave runtime create failed"))?,
        );
        self.completed_local_init_commit_transition_ids
            .insert(DAVE_PROTOCOL_INIT_TRANSITION_ID);
        self.pending_dave_local_init_commit_echoes
            .insert(DAVE_PROTOCOL_INIT_TRANSITION_ID, commit);
        Ok(())
    }

    async fn prepare_pending_initial_local_commit_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        operation: crate::dave::DaveMlsProposalsOperation,
        proposals: &[u8],
        pending_initial: PendingInitialDaveSession,
        transition_id: Option<u16>,
    ) -> Result<(), VoiceError> {
        let PendingInitialDaveSession {
            mut session,
            protocol_version,
        } = pending_initial;
        let external_sender =
            self.dave_external_sender
                .as_ref()
                .ok_or(VoiceError::InvalidState(
                    "voice dave external sender unavailable",
                ))?;
        let recognized_user_ids = self
            .dave_recognized_user_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let recognized = recognized_user_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let Some(commit_welcome) = session
            .process_proposals_with_operation(operation, proposals, &recognized)
            .map_err(|_| VoiceError::InvalidState("voice dave proposals invalid"))?
        else {
            self.pending_initial_dave = Some(PendingInitialDaveSession {
                session,
                protocol_version,
            });
            return Ok(());
        };
        let (commit, welcome) = external_sender
            .split_commit_welcome(&commit_welcome)
            .map_err(|_| VoiceError::InvalidState("voice dave commit welcome invalid"))?;
        tracing::debug!(
            commit_len = commit.len(),
            welcome_len = welcome.len(),
            recognized_user_ids = recognized.len(),
            "voice dave session committing pending initial proposals"
        );
        gateway
            .send_binary(protocol::dave_mls_commit_welcome_payload(&commit_welcome))
            .await?;
        let commit_result = session
            .process_commit(&commit)
            .map_err(|_| VoiceError::InvalidState("voice dave local commit invalid"))?;
        tracing::debug!(
            commit_failed = commit_result.is_failed(),
            commit_ignored = commit_result.is_ignored(),
            roster_member_ids = commit_result.roster_member_ids().len(),
            "voice dave session processed pending initial local commit"
        );
        if commit_result.is_failed()
            || commit_result.is_ignored()
            || commit_result.roster_member_ids().is_empty()
        {
            return Err(VoiceError::InvalidState(
                "voice dave local commit transition did not keep group",
            ));
        }
        self.dave = Some(
            DaveRuntimeContext::from_session(session)
                .map_err(|_| VoiceError::InvalidState("voice dave runtime create failed"))?,
        );
        match transition_id {
            Some(transition_id) if transition_id != DAVE_PROTOCOL_INIT_TRANSITION_ID => {
                self.consume_prepared_transition(transition_id)?;
                let runtime = self
                    .dave
                    .take()
                    .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))?;
                self.ready_pending_transition(
                    gateway,
                    transition_id,
                    runtime,
                    PendingDaveTransitionSource::CommitBacked,
                )
                .await
            }
            Some(transition_id) => {
                self.consume_prepared_transition(transition_id)?;
                self.completed_local_init_commit_transition_ids
                    .insert(transition_id);
                self.pending_dave_local_init_commit_echoes
                    .insert(transition_id, commit);
                Ok(())
            }
            None => {
                self.completed_local_init_commit_transition_ids
                    .insert(DAVE_PROTOCOL_INIT_TRANSITION_ID);
                self.pending_dave_local_init_commit_echoes
                    .insert(DAVE_PROTOCOL_INIT_TRANSITION_ID, commit);
                Ok(())
            }
        }
    }

    async fn ready_pending_transition(
        &mut self,
        gateway: &VoiceGatewayClient,
        transition_id: u16,
        runtime: DaveRuntimeContext,
        source: PendingDaveTransitionSource,
    ) -> Result<(), VoiceError> {
        self.pending_dave_transition = Some(PendingDaveTransition {
            transition_id,
            runtime,
            source,
        });
        if transition_id == DAVE_PROTOCOL_INIT_TRANSITION_ID {
            tracing::debug!(
                transition_id,
                "voice dave session not sending transition-ready for init transition"
            );
            return Ok(());
        }
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
        self.pending_initial_dave = None;
        self.pending_dave_local_commit_transition = None;
        self.pending_dave_prepared_transitions.clear();
        self.pending_dave_local_init_commit_echoes.clear();
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
                    .map(|pending_transition| pending_transition.runtime.protocol_version)
            })
            .or_else(|| {
                self.pending_dave_local_commit_transition
                    .as_ref()
                    .map(|runtime| runtime.protocol_version)
            })
            .or_else(|| {
                self.pending_initial_dave
                    .as_ref()
                    .map(|pending| pending.protocol_version)
            })
            .ok_or(VoiceError::InvalidState("voice dave runtime unavailable"))
    }

    fn mark_completed_transition(
        &mut self,
        transition_id: u16,
        source: PendingDaveTransitionSource,
    ) {
        if source == PendingDaveTransitionSource::WelcomeBacked {
            self.completed_welcome_backed_dave_transition_ids
                .insert(transition_id);
        }
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
        VoiceGatewayEvent::DaveMlsPrepareCommitTransition(_) => {
            "dave_mls_prepare_commit_transition"
        }
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
        dave_transition_ready_ids: Arc<Mutex<Vec<u16>>>,
    }

    impl FakeVoiceGateway {
        async fn spawn() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws_addr = listener.local_addr().unwrap();
            let speaking_observed = Arc::new(Notify::new());
            let speaking_observed_state = Arc::clone(&speaking_observed);
            let dave_transition_ready_ids = Arc::new(Mutex::new(Vec::new()));
            let dave_transition_ready_ids_state = Arc::clone(&dave_transition_ready_ids);

            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();

                while let Some(message) = ws.next().await {
                    let message = message.unwrap();
                    if let Message::Text(text) = message {
                        let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                        match payload.get("op").and_then(Value::as_u64) {
                            Some(5) => speaking_observed_state.notify_waiters(),
                            Some(23) => {
                                if let Some(transition_id) = payload
                                    .get("d")
                                    .and_then(|d| d.get("transition_id"))
                                    .and_then(Value::as_u64)
                                    .and_then(|transition_id| u16::try_from(transition_id).ok())
                                {
                                    dave_transition_ready_ids_state
                                        .lock()
                                        .await
                                        .push(transition_id);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            });

            Self {
                url: format!("ws://{ws_addr}/"),
                speaking_observed,
                dave_transition_ready_ids,
            }
        }

        fn url(&self) -> &str {
            &self.url
        }

        fn speaking_observed(&self) -> Arc<Notify> {
            Arc::clone(&self.speaking_observed)
        }

        async fn saw_dave_transition_ready(&self, transition_id: u16) -> bool {
            let ids = wait_for_value(&self.dave_transition_ready_ids, |ids| {
                ids.contains(&transition_id)
            })
            .await;
            ids.contains(&transition_id)
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

    #[tokio::test]
    async fn remote_commit_without_prepared_epoch_fails_before_runtime_lookup() {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let gateway_client = session.gateway.as_ref().expect("gateway").clone();

        let err = session
            .prepare_remote_commit_transition(&gateway_client, 42, &[1, 2, 3])
            .await
            .expect_err("unknown transition without prepared epoch must fail closed");

        assert_eq!(
            invalid_state_reason(err),
            "voice dave transition missing prepared epoch"
        );
    }

    #[tokio::test]
    async fn prepare_epoch_without_transition_id_fails_closed_on_active_session_path() {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let gateway_client = session.gateway.as_ref().expect("gateway").clone();
        let mut dave = crate::dave::DaveSession::new(None).expect("dave session");
        dave.init(1, 2, "1").expect("dave init");
        session.dave = Some(DaveRuntimeContext::from_session(dave).expect("dave runtime"));

        let err = session
            .process_gateway_event_for_dave(
                &gateway_client,
                VoiceGatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
                    transition_id: None,
                    epoch: "1".into(),
                    protocol_version: 1,
                }),
            )
            .await
            .expect_err("missing transition id must fail closed on active session path");

        assert_eq!(
            invalid_state_reason(err),
            "voice dave prepare epoch transition id missing"
        );
    }

    #[tokio::test]
    async fn remote_commit_replay_after_commit_backed_completion_still_requires_prepared_epoch() {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let gateway_client = session.gateway.as_ref().expect("gateway").clone();
        session.mark_completed_transition(7, PendingDaveTransitionSource::CommitBacked);

        let err = session
            .prepare_remote_commit_transition(&gateway_client, 7, &[1, 2, 3])
            .await
            .expect_err("commit-backed completion must not suppress replayed commit");

        assert_eq!(
            invalid_state_reason(err),
            "voice dave transition missing prepared epoch"
        );
    }

    #[tokio::test]
    async fn local_init_commit_echo_after_retry_does_not_send_transition_ready() {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let gateway_client = session.gateway.as_ref().expect("gateway").clone();
        let commit = vec![1, 2, 3];
        session
            .completed_local_init_commit_transition_ids
            .insert(DAVE_PROTOCOL_INIT_TRANSITION_ID);
        session
            .pending_dave_local_init_commit_echoes
            .insert(DAVE_PROTOCOL_INIT_TRANSITION_ID, commit.clone());

        session
            .prepare_remote_commit_transition(
                &gateway_client,
                DAVE_PROTOCOL_INIT_TRANSITION_ID,
                &commit,
            )
            .await
            .expect("matching local init commit echo should be accepted");

        assert!(
            !gateway
                .saw_dave_transition_ready(DAVE_PROTOCOL_INIT_TRANSITION_ID)
                .await
        );
    }

    #[tokio::test]
    async fn remote_commit_replay_after_welcome_backed_completion_ignores_unrelated_pending_epoch()
    {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let gateway_client = session.gateway.as_ref().expect("gateway").clone();
        session.mark_completed_transition(7, PendingDaveTransitionSource::WelcomeBacked);
        session.pending_dave_prepared_transitions.insert(8, 1);

        session
            .prepare_remote_commit_transition(&gateway_client, 7, &[1, 2, 3])
            .await
            .expect("welcome-backed replay should ignore stale commit even with unrelated pending epoch");

        assert_eq!(
            session.pending_dave_prepared_transitions.get(&8),
            Some(&1),
            "unrelated pending epochs must remain intact"
        );
    }

    #[tokio::test]
    async fn remote_welcome_replay_after_welcome_backed_completion_ignores_unrelated_pending_epoch()
    {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let gateway_client = session.gateway.as_ref().expect("gateway").clone();
        session.mark_completed_transition(7, PendingDaveTransitionSource::WelcomeBacked);
        session.pending_dave_prepared_transitions.insert(8, 1);

        session
            .prepare_welcome_transition(&gateway_client, 7, &[1, 2, 3])
            .await
            .expect(
                "welcome-backed replay should ignore stale welcome even with unrelated pending epoch",
            );

        assert_eq!(
            session.pending_dave_prepared_transitions.get(&8),
            Some(&1),
            "unrelated pending epochs must remain intact"
        );
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
            pending_initial_dave: None,
            dave_group_id: None,
            dave_external_sender: None,
            dave_external_sender_bytes: None,
            dave_recognized_user_ids: BTreeSet::from(["user-1".to_owned()]),
            completed_welcome_backed_dave_transition_ids: BTreeSet::new(),
            completed_local_init_commit_transition_ids: BTreeSet::new(),
            pending_dave_prepared_transitions: BTreeMap::new(),
            pending_dave_local_init_commit_echoes: BTreeMap::new(),
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

    fn invalid_state_reason(err: VoiceError) -> &'static str {
        match err {
            VoiceError::InvalidState(reason) => reason,
            other => panic!("expected invalid state error, got {other:?}"),
        }
    }
}
