use bytes::Bytes;
use serde_json::json;

use crate::error::VoiceError;
use crate::gateway::VoiceGatewayClient;
use crate::udp::VoiceUdpTransport;

pub const OPUS_SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];

pub async fn send_speaking(gateway: &VoiceGatewayClient, ssrc: u32) -> Result<(), VoiceError> {
    gateway
        .send_json(json!({
            "op": 5,
            "d": { "speaking": 1, "delay": 0, "ssrc": ssrc }
        }))
        .await
}

pub async fn send_stop_silence(transport: &mut VoiceUdpTransport) -> Result<(), VoiceError> {
    for _ in 0..5 {
        transport
            .send_audio_frame(Bytes::copy_from_slice(&OPUS_SILENCE_FRAME))
            .await?;
    }
    Ok(())
}
