use std::collections::HashMap;
use std::time::Duration;

#[allow(dead_code)]
#[path = "../src/bin/staging_live_check.rs"]
mod staging_live_check;

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Duration as TokioDuration, Instant, timeout};

use discord_voice_service::proto::discordvoice::v1::{SessionEvent, SessionEventKind};
use staging_live_check::{
    LiveContractState, LiveValidationEvidence, StagingConfig, combine_results,
    current_user_absent_from_guild_voice, leave_confirmed_by_rest_voice_state,
    wait_for_play_and_live_contract,
};
use twilight_http::Client as HttpClient;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, UserMarker},
};
use twilight_model::voice::VoiceState;

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
        (
            "DISCORD_VOICE_SERVICE_URI".to_owned(),
            "http://127.0.0.1:55051".to_owned(),
        ),
        (
            "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR".to_owned(),
            "http://127.0.0.1:50051".to_owned(),
        ),
    ]))
    .expect_err("config should fail");

    assert!(error.to_string().contains("BOT_TOKEN"));
}

#[test]
fn staging_controller_requires_service_uri() {
    let mut env = valid_env();
    env.remove("DISCORD_VOICE_SERVICE_URI");

    let error = StagingConfig::from_env_map(env).expect_err("config should fail");

    assert!(
        error.to_string().contains("DISCORD_VOICE_SERVICE_URI"),
        "expected DISCORD_VOICE_SERVICE_URI in error, got: {error}",
    );
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
fn evidence_json_captures_success_contract() {
    let evidence = LiveValidationEvidence {
        outcome: "success".to_owned(),
        service_uri: "http://127.0.0.1:55051".to_owned(),
        ytmusic_addr: "http://127.0.0.1:50051".to_owned(),
        saw_voice_ready: true,
        saw_playing: true,
        saw_track_ended: true,
        satisfied_min_interval: true,
        failure_reason: None,
    };

    let json = serde_json::to_string(&evidence).expect("evidence should serialize");

    assert!(json.contains("\"outcome\":\"success\""));
    assert!(json.contains("\"service_uri\":\"http://127.0.0.1:55051\""));
    assert!(json.contains("\"saw_track_ended\":true"));
}

#[test]
fn live_contract_requires_voice_ready_before_track_end() {
    let mut state = LiveContractState::default();
    let start = Instant::now();

    state
        .observe_event(event(SessionEventKind::Playing), start)
        .unwrap();
    state.update_min_interval(start + Duration::from_secs(5));

    let error = state
        .observe_event(
            event(SessionEventKind::TrackEnded),
            start + Duration::from_secs(5),
        )
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
    assert!(
        !state
            .observe_event(event(SessionEventKind::Playing), start)
            .unwrap()
    );
    state.update_min_interval(start + Duration::from_secs(5));

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

#[tokio::test]
async fn orchestration_waits_for_long_running_play_after_contract_success() {
    let (play_tx, play_rx) = oneshot::channel::<Result<()>>();
    let (contract_tx, contract_rx) = oneshot::channel::<Result<LiveContractState>>();

    let mut orchestration = tokio::spawn(wait_for_play_and_live_contract(
        async move { contract_rx.await.expect("contract sender should complete") },
        async move { play_rx.await.expect("play sender should complete") },
    ));

    contract_tx
        .send(Ok(LiveContractState::default()))
        .expect("contract result should be sent");

    assert!(
        timeout(TokioDuration::from_millis(25), &mut orchestration)
            .await
            .is_err(),
        "orchestration should keep waiting for play completion",
    );

    play_tx.send(Ok(())).expect("play result should be sent");

    orchestration.await.unwrap().unwrap();
}

#[tokio::test]
async fn orchestration_fails_immediately_when_play_errors_first() {
    let (play_tx, play_rx) = oneshot::channel::<Result<()>>();
    let (_contract_tx, contract_rx) = oneshot::channel::<Result<LiveContractState>>();

    let orchestration = tokio::spawn(wait_for_play_and_live_contract(
        async move {
            contract_rx
                .await
                .expect("contract sender should stay pending")
        },
        async move { play_rx.await.expect("play sender should complete") },
    ));

    play_tx
        .send(fail("call Play failed early"))
        .expect("play error should be sent");

    let error = orchestration
        .await
        .unwrap()
        .expect_err("play error should fail");
    assert!(error.to_string().contains("call Play failed early"));
}

#[tokio::test]
async fn orchestration_waits_for_contract_after_play_succeeds() {
    let (play_tx, play_rx) = oneshot::channel::<Result<()>>();
    let (contract_tx, contract_rx) = oneshot::channel::<Result<LiveContractState>>();

    let mut orchestration = tokio::spawn(wait_for_play_and_live_contract(
        async move { contract_rx.await.expect("contract sender should complete") },
        async move { play_rx.await.expect("play sender should complete") },
    ));

    play_tx.send(Ok(())).expect("play result should be sent");

    assert!(
        timeout(TokioDuration::from_millis(25), &mut orchestration)
            .await
            .is_err(),
        "orchestration should keep waiting for contract completion",
    );

    contract_tx
        .send(Ok(LiveContractState::default()))
        .expect("contract result should be sent");

    orchestration.await.unwrap().unwrap();
}

#[test]
fn cleanup_confirms_absence_when_voice_state_lookup_returns_not_found() {
    assert!(
        leave_confirmed_by_rest_voice_state(404, None).unwrap(),
        "404 voice state should mean the bot is already absent from guild voice",
    );
}

#[test]
fn cleanup_confirms_absence_when_voice_state_channel_is_none() {
    assert!(
        leave_confirmed_by_rest_voice_state(200, Some(&absent_voice_state())).unwrap(),
        "voice state with no channel should confirm cleanup",
    );
}

#[test]
fn cleanup_does_not_confirm_absence_when_voice_state_still_has_channel() {
    assert!(
        !leave_confirmed_by_rest_voice_state(200, Some(&present_voice_state())).unwrap(),
        "voice state with a channel should keep cleanup pending",
    );
}

#[tokio::test]
async fn cleanup_treats_http_404_voice_state_error_as_absence() {
    let client = mock_http_client(404, r#"{"message":"Unknown Voice State","code":10065}"#).await;

    assert!(
        current_user_absent_from_guild_voice(&client, Id::new(2))
            .await
            .unwrap(),
        "404 response errors from twilight-http should confirm cleanup absence",
    );
}

#[tokio::test]
async fn cleanup_keeps_non_404_voice_state_errors_strict() {
    let client = mock_http_client(500, r#"{"message":"Internal Server Error","code":0}"#).await;

    let error = current_user_absent_from_guild_voice(&client, Id::new(2))
        .await
        .expect_err("non-404 response errors should still fail cleanup");

    assert_eq!(
        error.to_string(),
        "query current user voice state during cleanup",
    );
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
            "DISCORD_VOICE_SERVICE_URI".to_owned(),
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

fn absent_voice_state() -> VoiceState {
    VoiceState {
        channel_id: None,
        deaf: false,
        guild_id: Some(Id::new(2)),
        member: None,
        mute: false,
        self_deaf: false,
        self_mute: false,
        self_stream: false,
        self_video: false,
        session_id: "session".to_owned(),
        suppress: false,
        user_id: Id::<UserMarker>::new(7),
        request_to_speak_timestamp: None,
    }
}

fn present_voice_state() -> VoiceState {
    VoiceState {
        channel_id: Some(Id::<ChannelMarker>::new(3)),
        ..absent_voice_state()
    }
}

async fn mock_http_client(status: u16, body: &'static str) -> HttpClient {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("server should accept");
        let response = format!(
            "HTTP/1.1 {status} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            reason_phrase(status),
            body.len(),
        );

        stream
            .write_all(response.as_bytes())
            .await
            .expect("server should write response");
    });

    HttpClient::builder()
        .proxy(address.to_string(), true)
        .build()
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    }
}
