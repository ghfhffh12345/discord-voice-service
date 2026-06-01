pub mod crypto;
pub mod dave;
mod discovery;
mod error;
mod gateway;
pub mod handshake;
mod protection;
mod protocol;
mod receive;
mod resume;
pub(crate) mod rollover;
mod rtp;
mod session;
mod speaking;
mod udp;
mod ws;

pub use error::VoiceError;
pub use receive::{ObservedAudioFrame, ObservedVoiceSession, PendingObservedVoiceSession};
pub use session::{ConnectedVoiceSession, VoiceContext};

#[doc(hidden)]
pub mod test_support {
    pub use crate::discovery::{
        build_ip_discovery_packet, discover_ip, parse_ip_discovery_response,
    };
    pub use crate::gateway::VoiceGatewayClient;
    pub use crate::protection::ProtectionContext;
    pub use crate::protocol::split_dave_mls_commit_welcome_payload;
    pub use crate::resume::GatewayEvent;
    pub use crate::rtp::{RtpPacketBuilder, parse_rtp_header};
    pub use crate::speaking::{OPUS_SILENCE_FRAME, send_speaking};
    pub use crate::udp::VoiceUdpTransport;
}
