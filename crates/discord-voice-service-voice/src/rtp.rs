use crate::error::VoiceError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtpHeader {
    pub version: u8,
    pub padding: bool,
    pub extension: bool,
    pub csrc_count: u8,
    pub marker: bool,
    pub payload_type: u8,
    pub sequence: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub header_len: usize,
}

pub struct RtpPacketBuilder {
    ssrc: u32,
}

impl RtpPacketBuilder {
    pub fn new(ssrc: u32) -> Self {
        Self { ssrc }
    }

    pub fn build_header(&self, sequence: u16, timestamp: u32) -> Vec<u8> {
        let mut packet = Vec::with_capacity(12);
        packet.push(0x80);
        packet.push(0x78);
        packet.extend_from_slice(&sequence.to_be_bytes());
        packet.extend_from_slice(&timestamp.to_be_bytes());
        packet.extend_from_slice(&self.ssrc.to_be_bytes());
        packet
    }

    pub fn build(&self, sequence: u16, timestamp: u32, payload: &[u8]) -> Vec<u8> {
        let mut packet = self.build_header(sequence, timestamp);
        packet.extend_from_slice(payload);
        packet
    }
}

pub fn parse_rtp_header(packet: &[u8]) -> Result<RtpHeader, VoiceError> {
    if packet.len() < 12 {
        return Err(VoiceError::InvalidState("voice rtp packet too short"));
    }

    let first = packet[0];
    let version = first >> 6;
    if version != 2 {
        return Err(VoiceError::InvalidState("voice rtp version unsupported"));
    }

    let padding = first & 0b0010_0000 != 0;
    let extension = first & 0b0001_0000 != 0;
    let csrc_count = first & 0b0000_1111;
    let second = packet[1];
    let marker = second & 0b1000_0000 != 0;
    let payload_type = second & 0b0111_1111;

    let mut header_len = 12 + usize::from(csrc_count) * 4;
    if packet.len() < header_len {
        return Err(VoiceError::InvalidState("voice rtp csrc list truncated"));
    }

    if extension {
        if packet.len() < header_len + 4 {
            return Err(VoiceError::InvalidState(
                "voice rtp extension header truncated",
            ));
        }
        let extension_len_words =
            u16::from_be_bytes([packet[header_len + 2], packet[header_len + 3]]);
        header_len += 4 + usize::from(extension_len_words) * 4;
        if packet.len() < header_len {
            return Err(VoiceError::InvalidState("voice rtp extension truncated"));
        }
    }

    Ok(RtpHeader {
        version,
        padding,
        extension,
        csrc_count,
        marker,
        payload_type,
        sequence: u16::from_be_bytes([packet[2], packet[3]]),
        timestamp: u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]),
        ssrc: u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]),
        header_len,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtpSequenceState {
    sequence: u16,
    timestamp: u32,
}

impl Default for RtpSequenceState {
    fn default() -> Self {
        Self::new()
    }
}

impl RtpSequenceState {
    pub fn new() -> Self {
        Self {
            sequence: 0,
            timestamp: 0,
        }
    }

    pub fn current(&self) -> (u16, u32) {
        (self.sequence, self.timestamp)
    }

    pub fn advance_by_samples(&mut self, timestamp_delta: u32) -> (u16, u32) {
        let current = (self.sequence, self.timestamp);
        self.sequence = self.sequence.wrapping_add(1);
        self.timestamp = self.timestamp.wrapping_add(timestamp_delta);
        current
    }
}
