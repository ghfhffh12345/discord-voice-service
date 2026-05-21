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

    pub fn advance(&mut self) -> (u16, u32) {
        let current = (self.sequence, self.timestamp);
        self.sequence = self.sequence.wrapping_add(1);
        self.timestamp = self.timestamp.wrapping_add(960);
        current
    }
}
