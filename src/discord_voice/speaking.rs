use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http::Uri;
use serde_json::json;

use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::udp::VoiceUdpTransport;
use crate::error::AppError;

pub const OPUS_SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];

pub async fn send_speaking(gateway: &mut VoiceGatewayClient, ssrc: u32) -> Result<(), AppError> {
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

pub struct ConnectedVoiceSession {
    gateway: VoiceGatewayClient,
    transport: VoiceUdpTransport,
    ssrc: u32,
    speaking_started: bool,
    speaking_flushed: bool,
}

impl ConnectedVoiceSession {
    pub async fn for_test(url: &str) -> Result<Self, AppError> {
        let uri: Uri = url.parse()?;
        let query = uri
            .path_and_query()
            .and_then(|path_and_query| path_and_query.query())
            .ok_or(AppError::InvalidState("voice session test query missing"))?;
        let udp = query_param(query, "udp")
            .ok_or(AppError::InvalidState("voice session test udp missing"))?;
        let ssrc = query_param(query, "ssrc")
            .ok_or(AppError::InvalidState("voice session test ssrc missing"))?
            .parse::<u32>()
            .map_err(|_| AppError::InvalidState("voice session test ssrc invalid"))?;
        let server = udp
            .parse::<SocketAddr>()
            .map_err(|_| AppError::InvalidState("voice session test udp invalid"))?;

        Ok(Self {
            gateway: VoiceGatewayClient::connect(url).await?,
            transport: VoiceUdpTransport::connect_with_ssrc(server, ssrc).await?,
            ssrc,
            speaking_started: false,
            speaking_flushed: false,
        })
    }

    pub async fn start_speaking(&mut self) -> Result<(), AppError> {
        if !self.speaking_started {
            send_speaking(&mut self.gateway, self.ssrc).await?;
            self.speaking_started = true;
            self.speaking_flushed = false;
        }
        Ok(())
    }

    pub async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), AppError> {
        self.start_speaking().await?;
        if !self.speaking_flushed {
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.speaking_flushed = true;
        }
        self.transport.send_audio_frame(frame).await
    }
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        (candidate == key).then_some(value)
    })
}
