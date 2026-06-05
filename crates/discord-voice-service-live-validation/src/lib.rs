mod audio;
mod config;
mod contract;
mod controller;
mod ytmusic_probe;

pub use audio::{
    AudioValidationAccumulator, AudioValidationStats, ObservedOpusPacket, analyze_opus_packets,
};
pub use config::StagingConfig;
pub use contract::{
    LiveContractState, LiveValidationEvidence, PlaybackBufferDepthEvidence,
    PlaybackDurationStatsEvidence, PlaybackStabilityEvidence, emit_validation_evidence,
    finalize_success_evidence,
};
pub use controller::{
    combine_results, current_user_absent_from_guild_voice, leave_confirmed_by_rest_voice_state,
    run, user_absent_from_guild_voice, wait_for_play_and_live_contract,
};
pub use ytmusic_probe::probe_ytmusic_public_grpc;
