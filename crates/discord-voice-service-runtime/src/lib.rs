mod api;
mod error;
mod observability;
mod session;

pub use api::ControlService;
pub use error::RuntimeError;
pub use session::{
    Command, DurationStatsSnapshot, PlaybackBufferDepthSnapshot, PlaybackQueueDepthStatsSnapshot,
    PlaybackSendCommandKind, PlaybackSendEventSnapshot, PlaybackStabilitySnapshot,
    PreparedPlayoutQueueEventKind, PreparedPlayoutQueueEventReason,
    PreparedPlayoutQueueEventSnapshot, PreparedTrackQueueDepthSampleSnapshot,
    PreparedTrackQueueSamplePhase, Readiness, ReadinessSnapshot, SessionState, Snapshot,
    Supervisor, VoiceContext,
};

pub fn record_ytmusic_probe(healthy: bool) {
    observability::global().record_ytmusic_probe(healthy);
}
