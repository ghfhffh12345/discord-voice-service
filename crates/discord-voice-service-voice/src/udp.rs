use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

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

pub struct VoiceUdpTransport {
    socket: UdpSocket,
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
            socket,
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

        let mut packet = vec![0u8; max_packet_len];
        let (len, from) = self.socket.recv_from(&mut packet).await?;
        if from != self.server {
            return Err(VoiceError::InvalidState("voice udp packet source invalid"));
        }
        packet.truncate(len);
        Ok(packet)
    }

    pub async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), VoiceError> {
        let (sequence, timestamp) = self.sequence.advance();
        let rtp_header = self.packet_builder.build_header(sequence, timestamp);
        let packet = match &self.protection {
            Some(protection) => protection.protect_packet(&rtp_header, frame.as_ref())?,
            None => {
                let mut packet = rtp_header;
                packet.extend_from_slice(frame.as_ref());
                packet
            }
        };
        self.socket.send_to(&packet, self.server).await?;
        Ok(())
    }

    pub async fn stop_audio(&mut self) -> Result<(), VoiceError> {
        crate::speaking::send_stop_silence(self).await
    }
}
