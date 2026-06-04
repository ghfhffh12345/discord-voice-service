use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

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
}

pub struct VoiceUdpTransport {
    socket: Arc<UdpSocket>,
    server: SocketAddr,
    packet_builder: RtpPacketBuilder,
    sequence: RtpSequenceState,
    local_addr: SocketAddr,
    protection: Option<ProtectionContext>,
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

        Ok(Self {
            socket: Arc::new(socket),
            server,
            packet_builder: RtpPacketBuilder::new(ssrc),
            sequence: RtpSequenceState::new(),
            local_addr,
            protection: None,
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
        self.protection = Some(protection);
        self
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn receive_packet(&self, max_packet_len: usize) -> Result<Vec<u8>, VoiceError> {
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
        if duration_samples == 0 {
            return Err(VoiceError::InvalidState(
                "voice audio frame duration invalid",
            ));
        }

        let (sequence, timestamp) = self.sequence.advance_by_samples(duration_samples);
        let rtp_header = self.packet_builder.build_header(sequence, timestamp);
        let packet = match &self.protection {
            Some(protection) => protection.protect_packet(&rtp_header, frame.as_ref())?,
            None => {
                let mut packet = rtp_header;
                packet.extend_from_slice(frame.as_ref());
                packet
            }
        };
        Ok(PreparedVoicePacket {
            bytes: Bytes::from(packet),
            duration_ms,
            duration_samples,
            is_track,
        })
    }

    pub async fn send_prepared_packet(
        &self,
        packet: &PreparedVoicePacket,
    ) -> Result<(), VoiceError> {
        self.socket
            .send_to(packet.bytes.as_ref(), self.server)
            .await?;
        Ok(())
    }

    pub async fn stop_audio(&mut self) -> Result<(), VoiceError> {
        crate::speaking::send_stop_silence(self).await
    }
}

fn duration_ms_from_samples(duration_samples: u32) -> u64 {
    u64::from(duration_samples).saturating_mul(1_000) / 48_000
}
