use std::net::SocketAddr;

use tokio::net::UdpSocket;

use crate::error::VoiceError;
use crate::udp::DiscoveredUdpAddress;

const DISCOVERY_PACKET_LEN: usize = 74;
const DISCOVERY_REQUEST_TYPE: u16 = 0x1;
const DISCOVERY_RESPONSE_TYPE: u16 = 0x2;
const DISCOVERY_BODY_LEN: u16 = 70;
const ADDRESS_START: usize = 8;
const ADDRESS_END: usize = 72;
const PORT_START: usize = 72;
const PORT_END: usize = 74;

pub async fn discover_ip(
    socket: &UdpSocket,
    server: SocketAddr,
    ssrc: u32,
) -> Result<DiscoveredUdpAddress, VoiceError> {
    let packet = build_ip_discovery_packet(ssrc);
    socket.send_to(&packet, server).await?;

    let mut buf = [0u8; DISCOVERY_PACKET_LEN];
    let (len, from) = socket.recv_from(&mut buf).await?;
    if from != server {
        return Err(VoiceError::InvalidState(
            "ip discovery response source did not match server",
        ));
    }
    if len < 8 {
        return Err(VoiceError::InvalidState(
            "ip discovery response shorter than expected",
        ));
    }
    let echoed_ssrc = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if echoed_ssrc != ssrc {
        return Err(VoiceError::InvalidState(
            "ip discovery response ssrc did not match request",
        ));
    }
    parse_ip_discovery_response(&buf[..len])
}

pub fn build_ip_discovery_packet(ssrc: u32) -> [u8; DISCOVERY_PACKET_LEN] {
    let mut packet = [0u8; DISCOVERY_PACKET_LEN];
    packet[..2].copy_from_slice(&DISCOVERY_REQUEST_TYPE.to_be_bytes());
    packet[2..4].copy_from_slice(&DISCOVERY_BODY_LEN.to_be_bytes());
    packet[4..8].copy_from_slice(&ssrc.to_be_bytes());
    packet
}

pub fn parse_ip_discovery_response(buf: &[u8]) -> Result<DiscoveredUdpAddress, VoiceError> {
    if buf.len() < DISCOVERY_PACKET_LEN {
        return Err(VoiceError::InvalidState(
            "ip discovery response shorter than expected",
        ));
    }

    let response_type = u16::from_be_bytes([buf[0], buf[1]]);
    if response_type != DISCOVERY_RESPONSE_TYPE {
        return Err(VoiceError::InvalidState(
            "ip discovery response type invalid",
        ));
    }

    let body_len = u16::from_be_bytes([buf[2], buf[3]]);
    if body_len != DISCOVERY_BODY_LEN {
        return Err(VoiceError::InvalidState(
            "ip discovery response length invalid",
        ));
    }

    let address_bytes = &buf[ADDRESS_START..ADDRESS_END];
    let address_end = address_bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(address_bytes.len());
    let ip = std::str::from_utf8(&address_bytes[..address_end])
        .map_err(|_| VoiceError::InvalidState("ip discovery response ip invalid utf-8"))?
        .to_owned();
    if ip.is_empty() {
        return Err(VoiceError::InvalidState("ip discovery response ip missing"));
    }

    let port = u16::from_be_bytes([buf[PORT_START], buf[PORT_END - 1]]);

    Ok(DiscoveredUdpAddress { ip, port })
}
