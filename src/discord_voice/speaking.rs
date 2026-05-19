use bytes::Bytes;
use serde_json::json;

use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::udp::VoiceUdpTransport;
use crate::error::AppError;

pub const OPUS_SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];

pub async fn send_speaking(gateway: &VoiceGatewayClient, ssrc: u32) -> Result<(), AppError> {
    gateway
        .send_json(json!({
            "op": 5,
            "d": { "speaking": 1, "delay": 0, "ssrc": ssrc }
        }))
        .await
}

pub async fn send_stop_silence(transport: &mut VoiceUdpTransport) -> Result<(), AppError> {
    for _ in 0..5 {
        transport
            .send_audio_frame(Bytes::copy_from_slice(&OPUS_SILENCE_FRAME))
            .await?;
    }
    Ok(())
}
