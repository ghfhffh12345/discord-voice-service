use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::net::UdpSocket;

use crate::discovery::discover_ip;
use crate::error::VoiceError;
use crate::protection::ProtectionContext;
use crate::rtp::{RtpPacketBuilder, RtpSequenceState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredUdpAddress {
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedVoicePacket {
    pub bytes: Bytes,
    pub duration_ms: u64,
    pub duration_samples: u32,
    pub is_track: bool,
    pub rtp_sequence: u16,
    pub rtp_timestamp: u32,
    pub protection_nonce: Option<u32>,
}

struct VoicePacketPreparer {
    packet_builder: RtpPacketBuilder,
    reserved_sequence: RtpSequenceState,
    protection: Option<ProtectionContext>,
}

impl VoicePacketPreparer {
    fn new(ssrc: u32) -> Self {
        Self {
            packet_builder: RtpPacketBuilder::new(ssrc),
            reserved_sequence: RtpSequenceState::new(),
            protection: None,
        }
    }

    fn with_protection(mut self, protection: ProtectionContext) -> Self {
        self.protection = Some(protection);
        self
    }

    fn prepare_audio_packet_with_duration_samples(
        &mut self,
        frame: Bytes,
        duration_ms: u64,
        duration_samples: u32,
    ) -> Result<PreparedVoicePacket, VoiceError> {
        if duration_samples == 0 {
            return Err(VoiceError::InvalidState(
                "voice audio frame duration invalid",
            ));
        }

        let (sequence, timestamp) = self.reserved_sequence.advance_by_samples(duration_samples);
        let rtp_header = self.packet_builder.build_header(sequence, timestamp);
        let packet = match &self.protection {
            Some(protection) => protection.protect_packet(&rtp_header, frame.as_ref())?,
            None => {
                let mut packet = rtp_header;
                packet.extend_from_slice(frame.as_ref());
                packet
            }
        };
        let protection_nonce = protection_nonce_from_packet(&packet, self.protection.is_some());
        Ok(PreparedVoicePacket {
            bytes: Bytes::from(packet),
            duration_ms,
            duration_samples,
            is_track: true,
            rtp_sequence: sequence,
            rtp_timestamp: timestamp,
            protection_nonce,
        })
    }

    fn discard_unsent_prepared_packets(&mut self, sent_cursor: RtpSequenceState) {
        self.reserved_sequence = sent_cursor;
    }
}

#[derive(Clone)]
pub struct VoicePreparedPacketSender {
    socket: Arc<UdpSocket>,
    server: SocketAddr,
    send_sequence: Arc<AtomicU64>,
}

impl VoicePreparedPacketSender {
    fn new(socket: Arc<UdpSocket>, server: SocketAddr) -> Self {
        Self {
            socket,
            server,
            send_sequence: Arc::new(AtomicU64::new(pack_rtp_cursor(RtpSequenceState::new()))),
        }
    }

    async fn receive_packet(&self, max_packet_len: usize) -> Result<Vec<u8>, VoiceError> {
        if max_packet_len == 0 {
            return Err(VoiceError::InvalidState("voice udp receive size invalid"));
        }

        loop {
            let mut packet = vec![0u8; max_packet_len];
            let (len, from) = self.socket.recv_from(&mut packet).await?;
            if from != self.server {
                tracing::debug!(source = %from, expected = %self.server, "ignoring stray voice udp packet");
                continue;
            }
            packet.truncate(len);
            return Ok(packet);
        }
    }

    pub async fn send_prepared_packet(
        &mut self,
        packet: &PreparedVoicePacket,
    ) -> Result<(), VoiceError> {
        let expected_cursor = pack_rtp_cursor_parts(packet.rtp_sequence, packet.rtp_timestamp);
        let (sequence, timestamp) =
            unpack_rtp_cursor(self.send_sequence.load(Ordering::Acquire)).current();
        if packet.rtp_sequence != sequence || packet.rtp_timestamp != timestamp {
            return Err(VoiceError::InvalidState(
                "voice prepared packet rtp cursor is not next send",
            ));
        }
        self.socket
            .send_to(packet.bytes.as_ref(), self.server)
            .await?;
        let next_cursor = pack_rtp_cursor_parts(
            packet.rtp_sequence.wrapping_add(1),
            packet.rtp_timestamp.wrapping_add(packet.duration_samples),
        );
        self.send_sequence
            .compare_exchange(
                expected_cursor,
                next_cursor,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                VoiceError::InvalidState("voice prepared packet send cursor advanced concurrently")
            })?;
        Ok(())
    }

    fn send_cursor(&self) -> RtpSequenceState {
        unpack_rtp_cursor(self.send_sequence.load(Ordering::Acquire))
    }
}

pub struct VoiceUdpTransport {
    preparer: VoicePacketPreparer,
    sender: VoicePreparedPacketSender,
    local_addr: SocketAddr,
}

impl VoiceUdpTransport {
    pub async fn connect(server: SocketAddr, ssrc: u32) -> Result<Self, VoiceError> {
        let bind_addr = match server {
            SocketAddr::V4(_) => SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            SocketAddr::V6(_) => "[::]:0"
                .parse()
                .map_err(|_| VoiceError::InvalidState("voice udp bind address invalid"))?,
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        let discovered_addr = discover_ip(&socket, server, ssrc).await?;
        let local_addr = SocketAddr::new(
            discovered_addr
                .ip
                .parse::<IpAddr>()
                .map_err(|_| VoiceError::InvalidState("discovered udp ip invalid"))?,
            discovered_addr.port,
        );

        let socket = Arc::new(socket);
        Ok(Self {
            preparer: VoicePacketPreparer::new(ssrc),
            sender: VoicePreparedPacketSender::new(socket, server),
            local_addr,
        })
    }

    pub async fn connect_protected(
        server: SocketAddr,
        ssrc: u32,
        protection: ProtectionContext,
    ) -> Result<Self, VoiceError> {
        Ok(Self::connect(server, ssrc)
            .await?
            .with_protection(protection))
    }

    pub fn with_protection(mut self, protection: ProtectionContext) -> Self {
        self.preparer = self.preparer.with_protection(protection);
        self
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn receive_packet(&self, max_packet_len: usize) -> Result<Vec<u8>, VoiceError> {
        self.sender.receive_packet(max_packet_len).await
    }

    pub async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), VoiceError> {
        self.send_audio_frame_with_duration_samples(frame, 960)
            .await
    }

    pub async fn send_audio_frame_with_duration_samples(
        &mut self,
        frame: Bytes,
        duration_samples: u32,
    ) -> Result<(), VoiceError> {
        let packet = self.prepare_audio_packet_with_duration_samples(
            frame,
            duration_ms_from_samples(duration_samples),
            duration_samples,
            true,
        )?;
        self.send_prepared_packet(&packet).await
    }

    pub fn prepare_audio_packet_with_duration_samples(
        &mut self,
        frame: Bytes,
        duration_ms: u64,
        duration_samples: u32,
        is_track: bool,
    ) -> Result<PreparedVoicePacket, VoiceError> {
        let mut packet = self.preparer.prepare_audio_packet_with_duration_samples(
            frame,
            duration_ms,
            duration_samples,
        )?;
        packet.is_track = is_track;
        Ok(packet)
    }

    pub fn prepared_packet_sender(&mut self) -> &mut VoicePreparedPacketSender {
        &mut self.sender
    }

    pub fn cloned_prepared_packet_sender(&self) -> VoicePreparedPacketSender {
        self.sender.clone()
    }

    pub fn discard_unsent_prepared_packets(&mut self) {
        self.preparer
            .discard_unsent_prepared_packets(self.sender.send_cursor());
    }

    pub async fn send_prepared_packet(
        &mut self,
        packet: &PreparedVoicePacket,
    ) -> Result<(), VoiceError> {
        self.sender.send_prepared_packet(packet).await
    }

    pub async fn stop_audio(&mut self) -> Result<(), VoiceError> {
        crate::speaking::send_stop_silence(self).await
    }
}

fn duration_ms_from_samples(duration_samples: u32) -> u64 {
    u64::from(duration_samples).saturating_mul(1_000) / 48_000
}

fn pack_rtp_cursor(state: RtpSequenceState) -> u64 {
    let (sequence, timestamp) = state.current();
    pack_rtp_cursor_parts(sequence, timestamp)
}

fn pack_rtp_cursor_parts(sequence: u16, timestamp: u32) -> u64 {
    (u64::from(timestamp) << 16) | u64::from(sequence)
}

fn unpack_rtp_cursor(cursor: u64) -> RtpSequenceState {
    RtpSequenceState::from_parts(cursor as u16, (cursor >> 16) as u32)
}

fn protection_nonce_from_packet(packet: &[u8], protected: bool) -> Option<u32> {
    if !protected {
        return None;
    }
    let nonce_suffix = packet.get(packet.len().saturating_sub(4)..)?;
    Some(u32::from_be_bytes([
        nonce_suffix[0],
        nonce_suffix[1],
        nonce_suffix[2],
        nonce_suffix[3],
    ]))
}
