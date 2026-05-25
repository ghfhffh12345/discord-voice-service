mod audio_match;
mod config;
mod contract;
mod controller;
mod observer;
mod ytmusic_probe;

pub use audio_match::{
    ObserverAudioEvidence, StreamingAudioMatcher, build_expected_track_frames,
    compare_expected_and_observed, compare_expected_and_observed_from_speaker,
};
pub use config::StagingConfig;
pub use contract::{
    LiveContractState, LiveValidationEvidence, emit_validation_evidence, finalize_success_evidence,
};
pub use controller::{
    combine_results, current_user_absent_from_guild_voice, leave_confirmed_by_rest_voice_state,
    run, wait_for_play_and_live_contract, wait_for_play_live_contract_and_observer,
    wait_for_play_live_contract_and_observer_task,
};
pub use observer::verify_observer_audio;
pub use ytmusic_probe::probe_ytmusic_public_grpc;
