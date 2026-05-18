pub struct RtpPacketBuilder {
    ssrc: u32,
}

impl RtpPacketBuilder {
    pub fn new(ssrc: u32) -> Self {
        Self { ssrc }
    }

    pub fn build(&self, sequence: u16, timestamp: u32, payload: &[u8]) -> Vec<u8> {
        let mut packet = Vec::with_capacity(12 + payload.len());
        packet.push(0x80);
        packet.push(0x78);
        packet.extend_from_slice(&sequence.to_be_bytes());
        packet.extend_from_slice(&timestamp.to_be_bytes());
        packet.extend_from_slice(&self.ssrc.to_be_bytes());
        packet.extend_from_slice(payload);
        packet
    }
}
