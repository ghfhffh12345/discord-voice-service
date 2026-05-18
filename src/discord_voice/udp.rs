use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

use bytes::Bytes;
use tokio::net::UdpSocket;

use crate::discord_voice::discovery::discover_ip;
use crate::discord_voice::rtp::{RtpPacketBuilder, RtpSequenceState};
use crate::error::AppError;

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
}

impl VoiceUdpTransport {
    pub async fn connect(server: SocketAddr, ssrc: u32) -> Result<Self, AppError> {
        let bind_addr = match server {
            SocketAddr::V4(_) => SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            SocketAddr::V6(_) => "[::]:0"
                .parse()
                .map_err(|_| AppError::InvalidState("voice udp bind address invalid"))?,
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        let discovered_addr = discover_ip(&socket, server, ssrc).await?;
        let local_addr = SocketAddr::new(
            discovered_addr
                .ip
                .parse::<IpAddr>()
                .map_err(|_| AppError::InvalidState("discovered udp ip invalid"))?,
            discovered_addr.port,
        );

        Ok(Self {
            socket,
            server,
            packet_builder: RtpPacketBuilder::new(ssrc),
            sequence: RtpSequenceState::new(),
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), AppError> {
        let (sequence, timestamp) = self.sequence.next();
        let packet = self
            .packet_builder
            .build(sequence, timestamp, frame.as_ref());
        self.socket.send_to(&packet, self.server).await?;
        Ok(())
    }

    pub async fn stop_audio(&mut self) -> Result<(), AppError> {
        crate::discord_voice::speaking::send_stop_silence(self).await
    }
}
