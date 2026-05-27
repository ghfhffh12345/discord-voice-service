use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};

use crate::dave::{DaveMediaType, DaveRuntimeContext};
use crate::error::VoiceError;
use crate::gateway::VoiceGatewayClient;
use crate::handshake;
use crate::protection::ProtectionContext;
use crate::protocol::{ClientDisconnect, Speaking, VoiceGatewayEvent, VoiceGatewayPayload};
use crate::rtp::{RtpHeader, parse_rtp_header};
use crate::session::VoiceContext;
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

pub struct ObservedVoiceSession {
    voice: VoiceContext,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
    protection: Option<ProtectionContext>,
    dave: Option<DaveRuntimeContext>,
    heartbeat_shutdown: Option<oneshot::Sender<()>>,
    speaker_ssrcs: HashMap<u32, String>,
}

impl ObservedVoiceSession {
    fn new(voice: VoiceContext) -> Self {
        Self {
            voice,
            gateway: None,
            transport: None,
            protection: None,
            dave: None,
            heartbeat_shutdown: None,
            speaker_ssrcs: HashMap::new(),
        }
    }

    pub async fn connect(voice: VoiceContext) -> Result<Self, VoiceError> {
        let Some(result) = handshake::connect(&voice).await? else {
            return Ok(Self::new(voice));
        };
        let handshake::VoiceHandshakeResult {
            gateway,
            transport,
            heartbeat_shutdown,
            session_description,
            dave,
            ..
        } = result;

        Ok(Self {
            voice,
            gateway: Some(gateway),
            transport: Some(transport),
            protection: Some(ProtectionContext::from_session(&session_description)?),
            dave: dave.map(|state| state.runtime),
            heartbeat_shutdown: Some(heartbeat_shutdown),
            speaker_ssrcs: HashMap::new(),
        })
    }

    pub fn voice_context(&self) -> &VoiceContext {
        &self.voice
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
            let gateway = self
                .gateway
                .as_ref()
                .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;
            let transport = self
                .transport
                .as_ref()
                .ok_or(VoiceError::InvalidState("voice transport unavailable"))?;

            tokio::select! {
                gateway_event = tokio::time::timeout(remaining, gateway.receive_event()) => {
                    let gateway_event = gateway_event
                        .map_err(|_| VoiceError::InvalidState("voice receive timed out"))??;
                    self.apply_gateway_payload(gateway_event);
                }
                packet = tokio::time::timeout(remaining, transport.receive_packet(MAX_UDP_PACKET_LEN)) => {
                    let packet = packet
                        .map_err(|_| VoiceError::InvalidState("voice receive timed out"))??;
                    let header = parse_rtp_header(&packet)?;
                    if !self.speaker_ssrcs.contains_key(&header.ssrc) {
                        self.wait_for_speaker_mapping(header.ssrc, deadline).await?;
                    }
                    let user_id = self
                        .speaker_ssrcs
                        .get(&header.ssrc)
                        .cloned()
                        .ok_or(VoiceError::InvalidState("voice speaker mapping missing"))?;
                    if expected_user_id.is_some_and(|expected| user_id != expected) {
                        continue;
                    }
                    return self.decode_audio_packet(&packet, header, user_id);
                }
            }
        }
    }

    fn apply_gateway_payload(&mut self, payload: VoiceGatewayPayload) {
        match payload.into_event() {
            VoiceGatewayEvent::Speaking(speaking) => self.apply_speaking(speaking),
            VoiceGatewayEvent::ClientDisconnect(disconnect) => {
                self.apply_client_disconnect(disconnect)
            }
            _ => {}
        }
    }

    fn apply_speaking(&mut self, speaking: Speaking) {
        if let Some(user_id) = speaking.user_id {
            self.record_speaker_ssrc(user_id, speaking.ssrc);
        }
    }

    fn apply_client_disconnect(&mut self, disconnect: ClientDisconnect) {
        self.speaker_ssrcs
            .retain(|_, user_id| user_id != &disconnect.user_id);
    }

    async fn wait_for_speaker_mapping(
        &mut self,
        ssrc: u32,
        deadline: Instant,
    ) -> Result<(), VoiceError> {
        let gateway = self
            .gateway
            .as_ref()
            .cloned()
            .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;
        while !self.speaker_ssrcs.contains_key(&ssrc) {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(VoiceError::InvalidState("voice receive timed out"));
            };
            let gateway_event = match tokio::time::timeout(remaining, gateway.receive_event()).await
            {
                Ok(Ok(event)) => event,
                Ok(Err(err)) => return Err(err),
                Err(_) => return Err(VoiceError::InvalidState("voice receive timed out")),
            };
            self.apply_gateway_payload(gateway_event);
        }
        Ok(())
    }

    fn decode_audio_packet(
        &mut self,
        packet: &[u8],
        header: RtpHeader,
        user_id: String,
    ) -> Result<ObservedAudioFrame, VoiceError> {
        let protection = self
            .protection
            .as_ref()
            .ok_or(VoiceError::InvalidState("voice protection unavailable"))?;
        let (_, payload) = protection.unprotect_packet(packet)?;
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
    ) -> Result<Bytes, VoiceError> {
        let Some(dave) = self.dave.as_mut() else {
            return Ok(payload);
        };

        let decrypted = dave
            .decrypt_audio_frame_from(user_id, DaveMediaType::Audio, payload.as_ref())
            .map_err(|_| VoiceError::InvalidState("voice dave frame decryption failed"))?;
        Ok(Bytes::from(decrypted))
    }
}

impl Drop for ObservedVoiceSession {
    fn drop(&mut self) {
        if let Some(shutdown) = self.heartbeat_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}
