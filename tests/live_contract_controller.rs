use std::collections::HashMap;
use std::time::Duration;

#[allow(dead_code)]
#[path = "../src/bin/staging_live_check.rs"]
mod staging_live_check;

use anyhow::Result;
use tokio::time::Instant;

use staging_live_check::{
    LiveContractState, SERVICE_FLOW_ORDER, ServiceFlowStep, StagingConfig, combine_results,
};
use discord_voice_service::proto::discordvoice::v1::{SessionEvent, SessionEventKind};

#[test]
fn staging_controller_requires_all_live_env_vars() {
    let error = StagingConfig::from_env_map(HashMap::new()).expect_err("config should fail");

    assert!(
        error.to_string().contains("BOT_TOKEN"),
        "expected BOT_TOKEN in error, got: {error}",
    );
}

#[test]
fn staging_controller_rejects_empty_required_values() {
    let error = StagingConfig::from_env_map(HashMap::from([
        ("BOT_TOKEN".to_owned(), "   ".to_owned()),
        ("APPLICATION_ID".to_owned(), "1".to_owned()),
        ("TEST_GUILD_ID".to_owned(), "2".to_owned()),
        ("TEST_VOICE_CHANNEL_ID".to_owned(), "3".to_owned()),
        ("TEST_VIDEO_ID".to_owned(), "video".to_owned()),
        ("DISCORD_VOICE_SERVICE_ADDR".to_owned(), "http://127.0.0.1:55051".to_owned()),
        (
            "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR".to_owned(),
            "http://127.0.0.1:50051".to_owned(),
        ),
    ]))
    .expect_err("config should fail");

    assert!(error.to_string().contains("BOT_TOKEN"));
}

#[test]
fn staging_controller_parses_valid_discord_ids() {
    let config = valid_config();

    assert_eq!(config.guild_id().unwrap().to_string(), "2");
    assert_eq!(config.channel_id().unwrap().to_string(), "3");
}

#[test]
fn staging_controller_rejects_invalid_discord_ids() {
    let mut env = valid_env();
    env.insert("TEST_GUILD_ID".to_owned(), "not-a-snowflake".to_owned());
    let config = StagingConfig::from_env_map(env).unwrap();

    let error = config.guild_id().expect_err("guild id should fail");

    assert!(error.to_string().contains("TEST_GUILD_ID"));
}

#[test]
fn service_flow_order_subscribes_before_join_and_play() {
    assert_eq!(
        SERVICE_FLOW_ORDER,
        [
            ServiceFlowStep::SubscribeEvents,
            ServiceFlowStep::JoinVoice,
            ServiceFlowStep::Play,
        ]
    );
}

#[test]
fn live_contract_requires_voice_ready_before_track_end() {
    let mut state = LiveContractState::default();
    let start = Instant::now();

    state
        .observe_event(event(SessionEventKind::Playing), start)
        .unwrap();
    state.mark_min_interval_elapsed();

    let error = state
        .observe_event(event(SessionEventKind::TrackEnded), start + Duration::from_secs(5))
        .expect_err("track end should fail");

    assert!(error.to_string().contains("VoiceReady"));
}

#[test]
fn live_contract_requires_five_seconds_of_playing() {
    let mut state = LiveContractState::default();
    let start = Instant::now();

    state
        .observe_event(event(SessionEventKind::VoiceReady), start)
        .unwrap();
    state
        .observe_event(event(SessionEventKind::Playing), start)
        .unwrap();

    let error = state
        .observe_event(
            event(SessionEventKind::TrackEnded),
            start + Duration::from_secs(4),
        )
        .expect_err("track end should fail");

    assert!(error.to_string().contains("5 seconds"));
}

#[test]
fn live_contract_passes_after_minimum_interval_and_track_end() {
    let mut state = LiveContractState::default();
    let start = Instant::now();

    state
        .observe_event(event(SessionEventKind::VoiceReady), start)
        .unwrap();
    assert!(!state
        .observe_event(event(SessionEventKind::Playing), start)
        .unwrap());
    state.mark_min_interval_elapsed();

    assert!(
        state
            .observe_event(
                event(SessionEventKind::TrackEnded),
                start + Duration::from_secs(5),
            )
            .unwrap()
    );
}

#[test]
fn live_contract_fails_on_reconnecting_after_playing() {
    let mut state = LiveContractState::default();
    let start = Instant::now();

    state
        .observe_event(event(SessionEventKind::VoiceReady), start)
        .unwrap();
    state
        .observe_event(event(SessionEventKind::Playing), start)
        .unwrap();

    let error = state
        .observe_event(
            event(SessionEventKind::VoiceReconnecting),
            start + Duration::from_secs(1),
        )
        .expect_err("reconnecting should fail");

    assert!(error.to_string().contains("VoiceReconnecting"));
}

#[test]
fn combine_results_preserves_both_primary_and_cleanup_failures() {
    let error = combine_results(fail("primary failed"), fail("cleanup failed"))
        .expect_err("combined result should fail");

    let message = error.to_string();
    assert!(message.contains("primary failed"));
    assert!(message.contains("cleanup also failed"));
    assert!(message.contains("cleanup failed"));
}

fn valid_config() -> StagingConfig {
    StagingConfig::from_env_map(valid_env()).unwrap()
}

fn valid_env() -> HashMap<String, String> {
    HashMap::from([
        ("BOT_TOKEN".to_owned(), "token".to_owned()),
        ("APPLICATION_ID".to_owned(), "1".to_owned()),
        ("TEST_GUILD_ID".to_owned(), "2".to_owned()),
        ("TEST_VOICE_CHANNEL_ID".to_owned(), "3".to_owned()),
        ("TEST_VIDEO_ID".to_owned(), "video".to_owned()),
        (
            "DISCORD_VOICE_SERVICE_ADDR".to_owned(),
            "http://127.0.0.1:55051".to_owned(),
        ),
        (
            "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR".to_owned(),
            "http://127.0.0.1:50051".to_owned(),
        ),
    ])
}

fn event(kind: SessionEventKind) -> SessionEvent {
    SessionEvent {
        kind: kind as i32,
        ..Default::default()
    }
}

fn fail(message: &'static str) -> Result<()> {
    Err(anyhow::anyhow!(message))
}
