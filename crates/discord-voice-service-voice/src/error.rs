use thiserror::Error;

#[derive(Debug, Error)]
pub enum VoiceError {
    #[error("unsupported encryption mode")]
    UnsupportedEncryptionMode,
    #[error("voice gateway op unsupported: {0}")]
    UnsupportedGatewayOp(u64),
    #[error("voice gateway binary opcode unsupported: {0}")]
    UnsupportedBinaryGatewayOp(u8),
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
}

impl VoiceError {
    pub fn is_gateway_closed_during_receive(&self) -> bool {
        matches!(
            self,
            Self::InvalidState("voice gateway closed during receive")
        )
    }

    pub fn is_packet_unprotect_failure(&self) -> bool {
        matches!(
            self,
            Self::InvalidState(
                "voice protected packet truncated"
                    | "voice protected packet body too short"
                    | "voice packet unprotect failed"
                    | "voice rtp padding invalid"
            )
        )
    }
}
