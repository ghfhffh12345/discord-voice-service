use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};
use tracing::debug;

use crate::dave::{DaveMediaType, DaveMlsProposalsOperation, DaveRuntimeContext};
use crate::error::VoiceError;
use crate::gateway::VoiceGatewayClient;
use crate::handshake;
use crate::protection::ProtectionContext;
use crate::protocol::{
    self, ClientDisconnect, ClientsConnect, DaveMlsPrepareCommitTransition, DaveMlsProposals,
    DaveMlsWelcome, Speaking, VoiceGatewayEvent, VoiceGatewayPayload,
};
use crate::rtp::{RtpHeader, parse_rtp_header};
use crate::session::VoiceContext;
use crate::speaking::OPUS_SILENCE_FRAME;
use crate::udp::VoiceUdpTransport;

const MAX_UDP_PACKET_LEN: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedAudioFrame {
    pub user_id: String,
    pub ssrc: u32,
    pub sequence: u16,
    pub timestamp: u32,
    pub payload: Bytes,
}

pub struct PendingObservedVoiceSession {
    voice: VoiceContext,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
    protection: Option<ProtectionContext>,
    dave: Option<handshake::PendingObserverDaveState>,
    dave_material: Option<handshake::ObserverDaveMaterial>,
    dave_recognized_user_ids: BTreeSet<String>,
    completed_dave_transition_ids: BTreeSet<u16>,
    dave_timeout: Option<Duration>,
    heartbeat_shutdown: Option<oneshot::Sender<()>>,
    speaker_ssrcs: HashMap<u32, String>,
    remote_speaker_candidates: BTreeSet<String>,
    pending_packets: HashMap<u32, PendingPacket>,
}

pub struct ObservedVoiceSession {
    voice: VoiceContext,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
    protection: Option<ProtectionContext>,
    dave: Option<DaveRuntimeContext>,
    author_dave_proposals: bool,
    dave_material: Option<handshake::ObserverDaveMaterial>,
    dave_recognized_user_ids: BTreeSet<String>,
    completed_dave_transition_ids: BTreeSet<u16>,
    heartbeat_shutdown: Option<oneshot::Sender<()>>,
    gateway_receive_closed: bool,
    pending_dave_proposals: Vec<(DaveMlsProposalsOperation, Vec<u8>)>,
    speaker_ssrcs: HashMap<u32, String>,
    remote_speaker_candidates: BTreeSet<String>,
    pending_packets: HashMap<u32, PendingPacket>,
}

struct PendingPacket {
    header: RtpHeader,
    packet: Vec<u8>,
}

enum DecodeAudioPacketError {
    NotReady,
    Fatal(VoiceError),
}

impl PendingObservedVoiceSession {
    fn new(voice: VoiceContext) -> Self {
        Self {
            voice,
            gateway: None,
            transport: None,
            protection: None,
            dave: None,
            dave_material: None,
            dave_recognized_user_ids: BTreeSet::new(),
            completed_dave_transition_ids: BTreeSet::new(),
            dave_timeout: None,
            heartbeat_shutdown: None,
            speaker_ssrcs: HashMap::new(),
            remote_speaker_candidates: BTreeSet::new(),
            pending_packets: HashMap::new(),
        }
    }

    pub async fn connect(voice: VoiceContext) -> Result<Self, VoiceError> {
        let Some(result) = handshake::connect_observer_participant(&voice).await? else {
            return Ok(Self::new(voice));
        };
        let handshake::PendingObserverHandshakeResult {
            gateway,
            transport,
            heartbeat_shutdown,
            session_description,
            dave_timeout,
            dave,
            ..
        } = result;

        Ok(Self {
            voice,
            gateway: Some(gateway),
            transport: Some(transport),
            protection: Some(ProtectionContext::from_session(&session_description)?),
            dave,
            dave_material: None,
            dave_recognized_user_ids: BTreeSet::new(),
            completed_dave_transition_ids: BTreeSet::new(),
            dave_timeout: Some(dave_timeout),
            heartbeat_shutdown: Some(heartbeat_shutdown),
            speaker_ssrcs: HashMap::new(),
            remote_speaker_candidates: BTreeSet::new(),
            pending_packets: HashMap::new(),
        })
    }

    pub async fn await_dave_ready(
        mut self,
        timeout_duration: Duration,
    ) -> Result<ObservedVoiceSession, VoiceError> {
        let dave = match self.dave.take() {
            Some(pending) => {
                let gateway = self
                    .gateway
                    .as_ref()
                    .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;
                let ready = handshake::complete_pending_observer_dave_join(
                    gateway,
                    pending,
                    timeout_duration,
                )
                .await?;
                self.dave_material = Some(ready.material);
                self.dave_recognized_user_ids = ready.recognized_user_ids.clone();
                self.remote_speaker_candidates = ready
                    .recognized_user_ids
                    .into_iter()
                    .filter(|user_id| user_id != &self.voice.user_id)
                    .collect();
                if let Some(transition_id) = ready.completed_transition_id {
                    self.completed_dave_transition_ids.insert(transition_id);
                }
                self.apply_pending_gateway_updates(ready.gateway_updates);
                Some(ready.runtime)
            }
            None => None,
        };

        Ok(ObservedVoiceSession {
            voice: self.voice.clone(),
            gateway: self.gateway.take(),
            transport: self.transport.take(),
            protection: self.protection.take(),
            dave,
            author_dave_proposals: true,
            dave_material: self.dave_material.take(),
            dave_recognized_user_ids: std::mem::take(&mut self.dave_recognized_user_ids),
            completed_dave_transition_ids: std::mem::take(&mut self.completed_dave_transition_ids),
            heartbeat_shutdown: self.heartbeat_shutdown.take(),
            gateway_receive_closed: false,
            pending_dave_proposals: Vec::new(),
            speaker_ssrcs: std::mem::take(&mut self.speaker_ssrcs),
            remote_speaker_candidates: std::mem::take(&mut self.remote_speaker_candidates),
            pending_packets: std::mem::take(&mut self.pending_packets),
        })
    }
}

impl ObservedVoiceSession {
    pub async fn connect(voice: VoiceContext) -> Result<Self, VoiceError> {
        let pending = PendingObservedVoiceSession::connect(voice).await?;
        let timeout = pending.dave_timeout.unwrap_or_default();
        pending.await_dave_ready(timeout).await
    }

    pub fn voice_context(&self) -> &VoiceContext {
        &self.voice
    }

    pub fn set_dave_proposal_authoring(&mut self, enabled: bool) {
        self.author_dave_proposals = enabled;
    }

    pub fn record_speaker_ssrc(&mut self, user_id: impl Into<String>, ssrc: u32) {
        self.speaker_ssrcs.insert(ssrc, user_id.into());
    }

    pub async fn receive_audio_frame(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<ObservedAudioFrame, VoiceError> {
        self.receive_audio_frame_internal(None, timeout_duration)
            .await
    }

    pub async fn receive_audio_frame_from(
        &mut self,
        expected_user_id: &str,
        timeout_duration: Duration,
    ) -> Result<ObservedAudioFrame, VoiceError> {
        self.receive_audio_frame_internal(Some(expected_user_id), timeout_duration)
            .await
    }

    async fn receive_audio_frame_internal(
        &mut self,
        expected_user_id: Option<&str>,
        timeout_duration: Duration,
    ) -> Result<ObservedAudioFrame, VoiceError> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(VoiceError::InvalidState("voice receive timed out"))?;
            let gateway = if self.gateway_receive_closed {
                None
            } else {
                Some(
                    self.gateway
                        .clone()
                        .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?,
                )
            };
            let transport = self
                .transport
                .as_ref()
                .ok_or(VoiceError::InvalidState("voice transport unavailable"))?;

            if let Some(gateway) = gateway {
                tokio::select! {
                    gateway_event = tokio::time::timeout(remaining, gateway.receive_event()) => {
                        let gateway_event = match gateway_event
                            .map_err(|_| VoiceError::InvalidState("voice receive timed out"))?
                        {
                            Ok(gateway_event) => gateway_event,
                            Err(err) if err.is_gateway_closed_during_receive() => {
                                self.mark_gateway_receive_closed();
                                continue;
                            }
                            Err(err) => return Err(err),
                        };
                        let remaining = deadline
                            .checked_duration_since(Instant::now())
                            .ok_or(VoiceError::InvalidState("voice receive timed out"))?;
                        self.apply_gateway_payload(gateway_event, remaining).await?;
                        if let Some(frame) = self.try_decode_pending_audio_frame(expected_user_id)? {
                            debug!(
                                user_id = %frame.user_id,
                                ssrc = frame.ssrc,
                                sequence = frame.sequence,
                                payload_len = frame.payload.len(),
                                "voice receive decoded audio frame"
                            );
                            return Ok(frame);
                        }
                    }
                    packet = tokio::time::timeout(remaining, transport.receive_packet(MAX_UDP_PACKET_LEN)) => {
                        let packet = packet
                            .map_err(|_| VoiceError::InvalidState("voice receive timed out"))??;
                        if let Some(frame) = self.process_audio_packet(packet, expected_user_id)? {
                            return Ok(frame);
                        }
                    }
                }
            } else {
                let packet =
                    tokio::time::timeout(remaining, transport.receive_packet(MAX_UDP_PACKET_LEN))
                        .await
                        .map_err(|_| VoiceError::InvalidState("voice receive timed out"))??;
                if let Some(frame) = self.process_audio_packet(packet, expected_user_id)? {
                    return Ok(frame);
                }
            }
        }
    }

    fn process_audio_packet(
        &mut self,
        packet: Vec<u8>,
        expected_user_id: Option<&str>,
    ) -> Result<Option<ObservedAudioFrame>, VoiceError> {
        if is_rtcp_packet(&packet) {
            debug!("voice receive ignored rtcp packet");
            return Ok(None);
        }
        let header = parse_rtp_header(&packet)?;
        debug!(
            ssrc = header.ssrc,
            sequence = header.sequence,
            timestamp = header.timestamp,
            has_mapping = self.speaker_ssrcs.contains_key(&header.ssrc),
            "voice receive observed udp packet"
        );
        if !self.speaker_ssrcs.contains_key(&header.ssrc) {
            if let (Some(expected_user_id), true) = (expected_user_id, self.dave.is_some()) {
                match self.decode_audio_packet(&packet, header, expected_user_id.to_owned()) {
                    Ok(frame) => {
                        debug!(
                            user_id = %expected_user_id,
                            ssrc = header.ssrc,
                            "voice receive inferred speaking mapping by expected user decrypt"
                        );
                        self.record_speaker_ssrc(expected_user_id, header.ssrc);
                        return Ok(Some(frame));
                    }
                    Err(DecodeAudioPacketError::NotReady) => {}
                    Err(DecodeAudioPacketError::Fatal(err)) => return Err(err),
                }
            }
            if let Some(user_id) = self.infer_single_remote_speaker(expected_user_id) {
                debug!(
                    user_id = %user_id,
                    ssrc = header.ssrc,
                    "voice receive inferred speaking mapping from single remote dave member"
                );
                self.record_speaker_ssrc(user_id, header.ssrc);
            } else {
                debug!(
                    ssrc = header.ssrc,
                    "voice receive buffered packet for unknown ssrc"
                );
                self.pending_packets
                    .insert(header.ssrc, PendingPacket { header, packet });
                return Ok(None);
            }
        }
        self.pending_packets.remove(&header.ssrc);
        let user_id = self
            .speaker_ssrcs
            .get(&header.ssrc)
            .cloned()
            .ok_or(VoiceError::InvalidState("voice speaker mapping missing"))?;
        if expected_user_id.is_some_and(|expected| user_id != expected) {
            debug!(
                expected_user_id,
                actual_user_id = %user_id,
                ssrc = header.ssrc,
                "voice receive ignored packet for unexpected user"
            );
            return Ok(None);
        }
        let frame = match self.decode_audio_packet(&packet, header, user_id) {
            Ok(frame) => frame,
            Err(DecodeAudioPacketError::NotReady) => {
                debug!(
                    ssrc = header.ssrc,
                    sequence = header.sequence,
                    "voice receive buffered packet until dave decrypt material arrives"
                );
                self.pending_packets
                    .insert(header.ssrc, PendingPacket { header, packet });
                return Ok(None);
            }
            Err(DecodeAudioPacketError::Fatal(err)) => return Err(err),
        };
        debug!(
            user_id = %frame.user_id,
            ssrc = frame.ssrc,
            sequence = frame.sequence,
            payload_len = frame.payload.len(),
            "voice receive decoded audio frame"
        );
        Ok(Some(frame))
    }

    fn mark_gateway_receive_closed(&mut self) {
        debug!("voice receive marking gateway receive closed during observation");
        if let Some(shutdown) = self.heartbeat_shutdown.take() {
            let _ = shutdown.send(());
        }
        self.gateway_receive_closed = true;
    }

    async fn apply_gateway_payload(
        &mut self,
        payload: VoiceGatewayPayload,
        timeout_duration: Duration,
    ) -> Result<(), VoiceError> {
        match payload.into_event() {
            VoiceGatewayEvent::Speaking(speaking) => self.apply_speaking(speaking),
            VoiceGatewayEvent::ClientsConnect(connect) => self.apply_clients_connect(connect),
            VoiceGatewayEvent::ClientDisconnect(disconnect) => {
                self.apply_client_disconnect(disconnect)
            }
            VoiceGatewayEvent::DaveMlsProposals(proposals) => {
                self.apply_dave_proposals(proposals).await?
            }
            VoiceGatewayEvent::DaveMlsWelcome(welcome) => {
                self.apply_dave_welcome(welcome, timeout_duration).await?
            }
            VoiceGatewayEvent::DaveMlsPrepareCommitTransition(commit) => {
                self.apply_dave_commit_transition(commit, timeout_duration)
                    .await?
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_speaking(&mut self, speaking: Speaking) {
        if let Some(user_id) = speaking.user_id {
            debug!(user_id = %user_id, ssrc = speaking.ssrc, "voice receive recorded speaking mapping");
            self.record_speaker_ssrc(user_id, speaking.ssrc);
        }
    }

    fn apply_clients_connect(&mut self, connect: ClientsConnect) {
        for user_id in connect.user_ids {
            if user_id != self.voice.user_id {
                self.remote_speaker_candidates.insert(user_id.clone());
            }
            self.dave_recognized_user_ids.insert(user_id);
        }
    }

    fn apply_client_disconnect(&mut self, disconnect: ClientDisconnect) {
        self.dave_recognized_user_ids.remove(&disconnect.user_id);
        self.remote_speaker_candidates.remove(&disconnect.user_id);
        self.speaker_ssrcs
            .retain(|_, user_id| user_id != &disconnect.user_id);
    }

    fn infer_single_remote_speaker(&self, expected_user_id: Option<&str>) -> Option<String> {
        let mut candidates = self.remote_speaker_candidates.iter();
        let candidate = candidates.next()?;
        if candidates.next().is_some() {
            return None;
        }
        match expected_user_id {
            Some(expected_user_id) if candidate != expected_user_id => None,
            _ if self
                .speaker_ssrcs
                .values()
                .any(|mapped_user_id| mapped_user_id == candidate) =>
            {
                None
            }
            _ => Some(candidate.clone()),
        }
    }

    async fn apply_dave_proposals(
        &mut self,
        proposals: DaveMlsProposals,
    ) -> Result<(), VoiceError> {
        if !self.author_dave_proposals {
            debug!(
                proposals_len = proposals.proposals.len(),
                recognized_user_ids = self.dave_recognized_user_ids.len(),
                "voice receive buffered dave proposals for an active sender"
            );
            self.pending_dave_proposals
                .push((proposals.operation, proposals.proposals));
            return Ok(());
        }
        let Some(dave) = self.dave.as_mut() else {
            return Ok(());
        };
        let recognized_user_ids = self
            .dave_recognized_user_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let recognized = recognized_user_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let commit_welcome = match dave.process_proposals_with_operation(
            proposals.operation,
            &proposals.proposals,
            &recognized,
        ) {
            Ok(commit_welcome) => commit_welcome,
            Err(err) => {
                debug!(
                    proposals_len = proposals.proposals.len(),
                    recognized_user_ids = recognized.len(),
                    error = ?err,
                    "voice receive ignored dave proposals"
                );
                return Ok(());
            }
        };
        let Some(commit_welcome) = commit_welcome else {
            debug!(
                proposals_len = proposals.proposals.len(),
                recognized_user_ids = recognized.len(),
                "voice receive ignored no-op dave proposals"
            );
            return Ok(());
        };
        let gateway = self
            .gateway
            .as_ref()
            .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;
        debug!(
            proposals_len = proposals.proposals.len(),
            recognized_user_ids = recognized.len(),
            commit_welcome_len = commit_welcome.len(),
            "voice receive sending dave commit welcome"
        );
        gateway
            .send_binary(protocol::dave_mls_commit_welcome_payload(&commit_welcome))
            .await
    }

    async fn apply_dave_welcome(
        &mut self,
        welcome: DaveMlsWelcome,
        timeout_duration: Duration,
    ) -> Result<(), VoiceError> {
        if self
            .completed_dave_transition_ids
            .contains(&welcome.transition_id)
        {
            debug!(
                transition_id = welcome.transition_id,
                "voice receive ignored replayed dave welcome transition"
            );
            return Ok(());
        }
        let Some(dave) = self.dave.as_mut() else {
            return Ok(());
        };
        if let Err(err) = dave.process_welcome(&welcome.welcome, &[]) {
            debug!(
                transition_id = welcome.transition_id,
                error = ?err,
                "voice receive ignored dave welcome transition"
            );
            return self
                .reinitialize_dave_after_invalid_transition(welcome.transition_id, timeout_duration)
                .await;
        }
        debug!(
            transition_id = welcome.transition_id,
            "voice receive applied dave welcome transition"
        );
        self.pending_dave_proposals.clear();
        if welcome.transition_id != 0 {
            self.gateway
                .as_ref()
                .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?
                .send_dave_transition_ready(welcome.transition_id)
                .await?;
        }
        self.completed_dave_transition_ids
            .insert(welcome.transition_id);
        Ok(())
    }

    async fn apply_dave_commit_transition(
        &mut self,
        commit: DaveMlsPrepareCommitTransition,
        timeout_duration: Duration,
    ) -> Result<(), VoiceError> {
        if self
            .completed_dave_transition_ids
            .contains(&commit.transition_id)
        {
            debug!(
                transition_id = commit.transition_id,
                "voice receive ignored replayed dave commit transition"
            );
            return Ok(());
        }
        let Some(mut dave) = self.dave.take() else {
            return Ok(());
        };
        if let Err(err) = self.stage_pending_dave_proposals(&mut dave) {
            self.dave = Some(dave);
            debug!(
                transition_id = commit.transition_id,
                error = ?err,
                "voice receive ignored dave commit transition after proposal staging failed"
            );
            return self
                .reinitialize_dave_after_invalid_transition(commit.transition_id, timeout_duration)
                .await;
        }
        if let Err(err) = dave.process_commit(&commit.commit) {
            self.dave = Some(dave);
            debug!(
                transition_id = commit.transition_id,
                error = ?err,
                "voice receive ignored dave commit transition"
            );
            return self
                .reinitialize_dave_after_invalid_transition(commit.transition_id, timeout_duration)
                .await;
        }
        debug!(
            transition_id = commit.transition_id,
            "voice receive applied dave commit transition"
        );
        self.dave = Some(dave);
        self.pending_dave_proposals.clear();
        if commit.transition_id != 0 {
            self.gateway
                .as_ref()
                .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?
                .send_dave_transition_ready(commit.transition_id)
                .await?;
        }
        self.completed_dave_transition_ids
            .insert(commit.transition_id);
        Ok(())
    }

    async fn reinitialize_dave_after_invalid_transition(
        &mut self,
        transition_id: u16,
        timeout_duration: Duration,
    ) -> Result<(), VoiceError> {
        let material = self.dave_material.clone().ok_or(VoiceError::InvalidState(
            "voice dave reinitialize material unavailable",
        ))?;
        let gateway = self
            .gateway
            .as_ref()
            .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;
        let pending = handshake::reinitialize_pending_observer_dave_join_after_invalid_transition(
            gateway,
            &self.voice,
            &material,
            self.dave_recognized_user_ids.clone(),
            transition_id,
        )
        .await?;
        self.dave = None;
        let ready =
            handshake::complete_pending_observer_dave_join(gateway, pending, timeout_duration)
                .await?;
        self.apply_observer_dave_ready(ready);
        Ok(())
    }

    fn apply_observer_dave_ready(&mut self, ready: handshake::PendingObserverReadyResult) {
        self.dave_material = Some(ready.material);
        self.dave_recognized_user_ids = ready.recognized_user_ids.clone();
        self.remote_speaker_candidates = ready
            .recognized_user_ids
            .into_iter()
            .filter(|user_id| user_id != &self.voice.user_id)
            .collect();
        if let Some(transition_id) = ready.completed_transition_id {
            self.completed_dave_transition_ids.insert(transition_id);
        }
        self.apply_pending_gateway_updates(ready.gateway_updates);
        self.pending_dave_proposals.clear();
        self.dave = Some(ready.runtime);
    }

    fn stage_pending_dave_proposals(
        &mut self,
        dave: &mut DaveRuntimeContext,
    ) -> Result<(), VoiceError> {
        if self.pending_dave_proposals.is_empty() {
            return Ok(());
        }
        let recognized_user_ids = self
            .dave_recognized_user_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let recognized = recognized_user_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let pending = std::mem::take(&mut self.pending_dave_proposals);
        for (operation, proposals) in pending {
            let commit_welcome = dave
                .process_proposals_with_operation(operation, &proposals, &recognized)
                .map_err(|_| VoiceError::InvalidState("voice observer dave proposals invalid"))?;
            debug!(
                proposals_len = proposals.len(),
                recognized_user_ids = recognized.len(),
                produced_commit_welcome = commit_welcome.is_some(),
                "voice receive staged buffered dave proposals before remote commit"
            );
        }
        Ok(())
    }

    fn apply_pending_gateway_updates(
        &mut self,
        updates: Vec<handshake::PendingObserverGatewayUpdate>,
    ) {
        for update in updates {
            match update {
                handshake::PendingObserverGatewayUpdate::Speaking { user_id, ssrc } => {
                    self.record_speaker_ssrc(user_id, ssrc);
                }
                handshake::PendingObserverGatewayUpdate::ClientDisconnect { user_id } => {
                    self.dave_recognized_user_ids.remove(&user_id);
                    self.remote_speaker_candidates.remove(&user_id);
                    self.speaker_ssrcs
                        .retain(|_, mapped_user_id| mapped_user_id != &user_id);
                }
            }
        }
    }

    fn try_decode_pending_audio_frame(
        &mut self,
        expected_user_id: Option<&str>,
    ) -> Result<Option<ObservedAudioFrame>, VoiceError> {
        let pending_ssrcs = self.pending_packets.keys().copied().collect::<Vec<_>>();
        for ssrc in pending_ssrcs {
            let Some(user_id) = self.speaker_ssrcs.get(&ssrc).cloned() else {
                continue;
            };
            let pending = self
                .pending_packets
                .remove(&ssrc)
                .ok_or(VoiceError::InvalidState("voice pending packet missing"))?;
            if expected_user_id.is_some_and(|expected| user_id != expected) {
                debug!(
                    expected_user_id,
                    actual_user_id = %user_id,
                    ssrc,
                    "voice receive ignored packet for unexpected user"
                );
                continue;
            }
            match self.decode_audio_packet(&pending.packet, pending.header, user_id) {
                Ok(frame) => return Ok(Some(frame)),
                Err(DecodeAudioPacketError::NotReady) => {
                    debug!(
                        ssrc,
                        "voice receive keeping pending packet until dave decrypt material arrives"
                    );
                    self.pending_packets.insert(ssrc, pending);
                    continue;
                }
                Err(DecodeAudioPacketError::Fatal(err)) => return Err(err),
            }
        }
        Ok(None)
    }

    fn decode_audio_packet(
        &mut self,
        packet: &[u8],
        header: RtpHeader,
        user_id: String,
    ) -> Result<ObservedAudioFrame, DecodeAudioPacketError> {
        let protection = self
            .protection
            .as_ref()
            .ok_or(VoiceError::InvalidState("voice protection unavailable"))
            .map_err(DecodeAudioPacketError::Fatal)?;
        let (_, payload) = protection
            .unprotect_packet(packet)
            .map_err(DecodeAudioPacketError::Fatal)?;
        let payload = self.decrypt_audio_payload(&user_id, payload)?;

        Ok(ObservedAudioFrame {
            user_id,
            ssrc: header.ssrc,
            sequence: header.sequence,
            timestamp: header.timestamp,
            payload,
        })
    }

    fn decrypt_audio_payload(
        &mut self,
        user_id: &str,
        payload: Bytes,
    ) -> Result<Bytes, DecodeAudioPacketError> {
        let Some(dave) = self.dave.as_mut() else {
            return Ok(payload);
        };
        if payload.as_ref() == OPUS_SILENCE_FRAME.as_slice() {
            return Ok(payload);
        }

        let decrypted = dave
            .decrypt_audio_frame_from(user_id, DaveMediaType::Audio, payload.as_ref())
            .map_err(|_| DecodeAudioPacketError::NotReady)?;
        Ok(Bytes::from(decrypted))
    }
}

impl PendingObservedVoiceSession {
    fn apply_pending_gateway_updates(
        &mut self,
        updates: Vec<handshake::PendingObserverGatewayUpdate>,
    ) {
        for update in updates {
            match update {
                handshake::PendingObserverGatewayUpdate::Speaking { user_id, ssrc } => {
                    self.record_speaker_ssrc(user_id, ssrc);
                }
                handshake::PendingObserverGatewayUpdate::ClientDisconnect { user_id } => {
                    self.remote_speaker_candidates.remove(&user_id);
                    self.speaker_ssrcs
                        .retain(|_, mapped_user_id| mapped_user_id != &user_id);
                }
            }
        }
    }

    fn record_speaker_ssrc(&mut self, user_id: impl Into<String>, ssrc: u32) {
        self.speaker_ssrcs.insert(ssrc, user_id.into());
    }
}

fn is_rtcp_packet(packet: &[u8]) -> bool {
    packet
        .get(1)
        .is_some_and(|packet_type| (192..=223).contains(packet_type))
}

impl Drop for ObservedVoiceSession {
    fn drop(&mut self) {
        if let Some(shutdown) = self.heartbeat_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Drop for PendingObservedVoiceSession {
    fn drop(&mut self) {
        if let Some(shutdown) = self.heartbeat_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}
