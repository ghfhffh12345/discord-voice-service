#![recursion_limit = "256"]

use anyhow::Result;
use discord_voice_service_live_validation::{
    LiveContractState, LiveValidationEvidence, PlaybackBufferDepthEvidence,
    PlaybackDurationStatsEvidence, PlaybackQueueDepthStatsEvidence, PlaybackStabilityEvidence,
    StagingConfig, combine_results, current_user_absent_from_guild_voice,
    finalize_success_evidence, leave_confirmed_by_rest_voice_state, user_absent_from_guild_voice,
    wait_for_play_and_live_contract,
};
use discord_voice_service_twilight::{SessionEvent, SessionEventKind};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Duration as TokioDuration, timeout};
use twilight_http::Client as HttpClient;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, UserMarker},
};
use twilight_model::voice::VoiceState;

#[test]
fn staging_controller_requires_observer_live_env_vars() {
    let error = StagingConfig::from_env_map(HashMap::new()).expect_err("config should fail");

    assert!(
        error.to_string().contains("OBSERVER_BOT_TOKEN"),
        "expected OBSERVER_BOT_TOKEN in error, got: {error}",
    );
}

#[test]
fn staging_controller_rejects_empty_required_values() {
    let error = StagingConfig::from_env_map(HashMap::from([
        ("BOT_TOKEN".to_owned(), "   ".to_owned()),
        ("OBSERVER_BOT_TOKEN".to_owned(), "observer-token".to_owned()),
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
        (
            "LIVE_STAGING_PROFILE".to_owned(),
            "constrained-github-hosted".to_owned(),
        ),
        ("LIVE_STAGING_SERVICE_CPUS".to_owned(), "1.0".to_owned()),
        (
            "LIVE_STAGING_CPU_CONTENTION_WORKERS".to_owned(),
            "2".to_owned(),
        ),
        ("LIVE_STAGING_HTTP_READ_DELAY_MS".to_owned(), "5".to_owned()),
        (
            "LIVE_STAGING_HTTP_READ_JITTER_MS".to_owned(),
            "25".to_owned(),
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
fn staging_controller_rejects_renamed_constrained_profile() {
    let mut env = valid_env();
    env.insert(
        "LIVE_STAGING_PROFILE".to_owned(),
        "relaxed-local".to_owned(),
    );

    let error = StagingConfig::from_env_map(env).expect_err("config should fail");

    assert!(
        error.to_string().contains("LIVE_STAGING_PROFILE"),
        "expected profile validation failure, got: {error}",
    );
}

#[test]
fn staging_controller_rejects_increased_service_cpu_allocation() {
    let mut env = valid_env();
    env.insert("LIVE_STAGING_SERVICE_CPUS".to_owned(), "1.5".to_owned());

    let error = StagingConfig::from_env_map(env).expect_err("config should fail");

    assert!(
        error.to_string().contains("LIVE_STAGING_SERVICE_CPUS"),
        "expected service CPU validation failure, got: {error}",
    );
}

#[test]
fn staging_controller_rejects_disabled_cpu_contention() {
    let mut env = valid_env();
    env.insert(
        "LIVE_STAGING_CPU_CONTENTION_WORKERS".to_owned(),
        "1".to_owned(),
    );

    let error = StagingConfig::from_env_map(env).expect_err("config should fail");

    assert!(
        error
            .to_string()
            .contains("LIVE_STAGING_CPU_CONTENTION_WORKERS"),
        "expected CPU contention validation failure, got: {error}",
    );
}

#[test]
fn staging_controller_rejects_weakened_http_read_stress() {
    let mut env = valid_env();
    env.insert("LIVE_STAGING_HTTP_READ_DELAY_MS".to_owned(), "4".to_owned());

    let error = StagingConfig::from_env_map(env).expect_err("config should fail");

    assert!(
        error
            .to_string()
            .contains("LIVE_STAGING_HTTP_READ_DELAY_MS"),
        "expected HTTP delay validation failure, got: {error}",
    );

    let mut env = valid_env();
    env.insert(
        "LIVE_STAGING_HTTP_READ_JITTER_MS".to_owned(),
        "24".to_owned(),
    );

    let error = StagingConfig::from_env_map(env).expect_err("config should fail");

    assert!(
        error
            .to_string()
            .contains("LIVE_STAGING_HTTP_READ_JITTER_MS"),
        "expected HTTP jitter validation failure, got: {error}",
    );
}

#[test]
fn staging_controller_redacts_tokens_in_debug_output() {
    let config = valid_config();
    let debug = format!("{config:?}");

    assert!(debug.contains("bot_token: \"[REDACTED]\""));
    assert!(debug.contains("observer_bot_token: \"[REDACTED]\""));
    assert!(!debug.contains("bot_token: \"token\""));
    assert!(!debug.contains("observer_bot_token: \"observer-token\""));
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
fn evidence_json_captures_receive_side_success_contract() {
    let evidence = LiveValidationEvidence {
        outcome: "success".to_owned(),
        service_uri: "http://127.0.0.1:55051".to_owned(),
        ytmusic_addr: "http://127.0.0.1:50051".to_owned(),
        test_video_id: "video".to_owned(),
        expected_track_duration_ms: 180_000,
        active_validation_duration_after_resume_ms: 150_000,
        pause_silence_packet_count: 5,
        pause_silence_spacing_ms: vec![20, 20, 20, 20],
        live_staging_profile: "constrained-github-hosted".to_owned(),
        live_staging_service_cpus: "1.0".to_owned(),
        live_staging_cpu_contention_workers: 2,
        live_staging_http_read_delay_ms: 5,
        live_staging_http_read_jitter_ms: 25,
        validated_join_voice: true,
        validated_update_voice_context: true,
        validated_play: true,
        validated_pause: true,
        validated_resume: true,
        validated_invalid_resume_ignored: true,
        validated_redundant_pause_ignored: true,
        observer_proved_pause: true,
        observer_proved_resume: true,
        observer_pause_self_mute_observed: false,
        observer_pause_speaking_stopped: true,
        observer_pause_rtp_silence_observed: true,
        observer_resume_speaking_started: true,
        observer_pause_silence_ms: 600,
        observer_resume_packet_count: 4,
        validated_reconnect_rollover_during_playback: true,
        validated_stop: true,
        validated_stop_during_playback: true,
        validated_leave_voice: true,
        validated_leave_voice_during_playback: true,
        validated_get_state: true,
        validated_get_playback_metrics: true,
        validated_subscribe_events: true,
        saw_voice_connecting: true,
        saw_voice_ready: true,
        saw_track_resolving: true,
        saw_playing: true,
        saw_track_ended: true,
        observed_packet_count: 144,
        decoded_audio_ms: 3200,
        observer_wall_clock_elapsed_ms: 3200,
        observer_decoded_audio_to_wall_clock_ratio_ppm: 1_000_000,
        non_silent_audio_ms: 1800,
        observer_rtp_inter_arrival: playback_stats(143, 20, 22, 25, 19, 28),
        observer_rtp_gap_count_gte_100ms: 0,
        observer_rtp_fast_interval_count: 0,
        observer_rtp_fast_interval_min_ms: 0,
        observer_rtp_fast_interval_min_us: 0,
        observer_decoded_audio_tempo_window_count: 95,
        observer_decoded_audio_tempo_window_post_source_buffer_count: 1,
        observer_decoded_audio_tempo_window_min_ratio_ppm: 1_000_000,
        observer_decoded_audio_tempo_window_max_ratio_ppm: 1_000_000,
        observer_decoded_audio_tempo_window_fast_count: 0,
        observer_decoded_audio_tempo_window_fastest_ratio_ppm: 0,
        observer_decoded_audio_tempo_window_fastest_media_ms: 0,
        observer_decoded_audio_tempo_window_fastest_wall_clock_us: 0,
        observer_decoded_audio_tempo_window_slow_count: 0,
        observer_decoded_audio_tempo_window_slowest_ratio_ppm: 0,
        observer_decoded_audio_tempo_window_slowest_media_ms: 0,
        observer_decoded_audio_tempo_window_slowest_wall_clock_us: 0,
        dave_transition_count_during_playback: 0,
        playback_metrics: Some(sample_playback_metrics()),
        reconnect_probe_metrics: Some(sample_reconnect_probe_metrics()),
        validated_constrained_profile: true,
        validated_slow_jittery_http: true,
        failure_reason: None,
    };

    let json = serde_json::to_value(&evidence).expect("evidence should serialize");
    let playback_metrics =
        serde_json::to_value(sample_playback_metrics()).expect("metrics should serialize");
    let reconnect_probe_metrics =
        serde_json::to_value(sample_reconnect_probe_metrics()).expect("metrics should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "outcome": "success",
            "service_uri": "http://127.0.0.1:55051",
            "ytmusic_addr": "http://127.0.0.1:50051",
            "test_video_id": "video",
            "expected_track_duration_ms": 180_000,
            "active_validation_duration_after_resume_ms": 150_000,
            "pause_silence_packet_count": 5,
            "pause_silence_spacing_ms": [20, 20, 20, 20],
            "live_staging_profile": "constrained-github-hosted",
            "live_staging_service_cpus": "1.0",
            "live_staging_cpu_contention_workers": 2,
            "live_staging_http_read_delay_ms": 5,
            "live_staging_http_read_jitter_ms": 25,
            "validated_join_voice": true,
            "validated_update_voice_context": true,
            "validated_play": true,
            "validated_pause": true,
            "validated_resume": true,
            "validated_invalid_resume_ignored": true,
            "validated_redundant_pause_ignored": true,
            "observer_proved_pause": true,
            "observer_proved_resume": true,
            "observer_pause_self_mute_observed": false,
            "observer_pause_speaking_stopped": true,
            "observer_pause_rtp_silence_observed": true,
            "observer_resume_speaking_started": true,
            "observer_pause_silence_ms": 600,
            "observer_resume_packet_count": 4,
            "validated_reconnect_rollover_during_playback": true,
            "validated_stop": true,
            "validated_stop_during_playback": true,
            "validated_leave_voice": true,
            "validated_leave_voice_during_playback": true,
            "validated_get_state": true,
            "validated_get_playback_metrics": true,
            "validated_subscribe_events": true,
            "saw_voice_connecting": true,
            "saw_voice_ready": true,
            "saw_track_resolving": true,
            "saw_playing": true,
            "saw_track_ended": true,
            "observed_packet_count": 144,
            "decoded_audio_ms": 3200,
            "observer_wall_clock_elapsed_ms": 3200,
            "observer_decoded_audio_to_wall_clock_ratio_ppm": 1_000_000,
            "non_silent_audio_ms": 1800,
            "observer_rtp_inter_arrival": {
                "samples": 143,
                "p50_ms": 20,
                "p95_ms": 22,
                "p99_ms": 25,
                "min_ms": 19,
                "max_ms": 28,
            },
            "observer_rtp_gap_count_gte_100ms": 0,
            "observer_rtp_fast_interval_count": 0,
            "observer_rtp_fast_interval_min_ms": 0,
            "observer_rtp_fast_interval_min_us": 0,
            "observer_decoded_audio_tempo_window_count": 95,
            "observer_decoded_audio_tempo_window_post_source_buffer_count": 1,
            "observer_decoded_audio_tempo_window_min_ratio_ppm": 1_000_000,
            "observer_decoded_audio_tempo_window_max_ratio_ppm": 1_000_000,
            "observer_decoded_audio_tempo_window_fast_count": 0,
            "observer_decoded_audio_tempo_window_fastest_ratio_ppm": 0,
            "observer_decoded_audio_tempo_window_fastest_media_ms": 0,
            "observer_decoded_audio_tempo_window_fastest_wall_clock_us": 0,
            "observer_decoded_audio_tempo_window_slow_count": 0,
            "observer_decoded_audio_tempo_window_slowest_ratio_ppm": 0,
            "observer_decoded_audio_tempo_window_slowest_media_ms": 0,
            "observer_decoded_audio_tempo_window_slowest_wall_clock_us": 0,
            "dave_transition_count_during_playback": 0,
            "playback_metrics": playback_metrics,
            "reconnect_probe_metrics": reconnect_probe_metrics,
            "validated_constrained_profile": true,
            "validated_slow_jittery_http": true,
            "failure_reason": null,
        })
    );
}

#[test]
fn live_contract_requires_voice_ready_before_track_end() {
    let mut state = LiveContractState::default();

    state
        .observe_event(event(SessionEventKind::Playing, Some("video")), "video")
        .unwrap();

    let error = state
        .observe_event(event(SessionEventKind::TrackEnded, Some("video")), "video")
        .expect_err("track end should fail");

    assert!(error.to_string().contains("VoiceReady"));
}

#[test]
fn live_contract_requires_playing_before_track_end() {
    let mut state = LiveContractState::default();

    state
        .observe_event(event(SessionEventKind::VoiceReady, None), "video")
        .unwrap();

    let error = state
        .observe_event(event(SessionEventKind::TrackEnded, Some("video")), "video")
        .expect_err("track end should fail");

    assert!(error.to_string().contains("Playing"));
}

#[test]
fn live_contract_passes_when_track_ends_after_voice_ready_and_playing() {
    let mut state = LiveContractState::default();

    state
        .observe_event(event(SessionEventKind::VoiceReady, None), "video")
        .unwrap();
    state
        .observe_event(
            event(SessionEventKind::TrackResolving, Some("video")),
            "video",
        )
        .unwrap();
    assert!(
        !state
            .observe_event(event(SessionEventKind::Playing, Some("video")), "video")
            .unwrap()
    );

    assert!(
        state
            .observe_event(event(SessionEventKind::TrackEnded, Some("video")), "video")
            .unwrap()
    );
}

#[test]
fn live_contract_fails_when_track_end_video_id_differs_after_playing() {
    let mut state = LiveContractState::default();

    state
        .observe_event(event(SessionEventKind::VoiceReady, None), "video")
        .unwrap();
    state
        .observe_event(
            event(SessionEventKind::TrackResolving, Some("video")),
            "video",
        )
        .unwrap();
    state
        .observe_event(event(SessionEventKind::Playing, Some("video")), "video")
        .unwrap();

    let error = state
        .observe_event(
            event(SessionEventKind::TrackEnded, Some("other-video")),
            "video",
        )
        .expect_err("mismatched track end should fail");

    assert!(error.to_string().contains("expected video"));
}

#[test]
fn live_contract_fails_when_playing_video_id_differs_from_expected() {
    let mut state = LiveContractState::default();

    state
        .observe_event(event(SessionEventKind::VoiceReady, None), "video")
        .unwrap();
    state
        .observe_event(
            event(SessionEventKind::TrackResolving, Some("video")),
            "video",
        )
        .unwrap();

    let error = state
        .observe_event(
            event(SessionEventKind::Playing, Some("other-video")),
            "video",
        )
        .expect_err("wrong playing video should fail");

    assert!(error.to_string().contains("expected video"));
}

#[test]
fn live_contract_fails_on_reconnecting_after_playing() {
    let mut state = LiveContractState::default();

    state
        .observe_event(event(SessionEventKind::VoiceReady, None), "video")
        .unwrap();
    state
        .observe_event(
            event(SessionEventKind::TrackResolving, Some("video")),
            "video",
        )
        .unwrap();
    state
        .observe_event(event(SessionEventKind::Playing, Some("video")), "video")
        .unwrap();

    let error = state
        .observe_event(
            event(SessionEventKind::VoiceReconnecting, Some("video")),
            "video",
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

#[test]
fn success_evidence_is_emitted_even_when_cleanup_confirmation_lags() {
    let built = Arc::new(AtomicBool::new(false));
    let emitted = Arc::new(AtomicBool::new(false));
    let built_for_closure = Arc::clone(&built);
    let emitted_for_closure = Arc::clone(&emitted);

    finalize_success_evidence(
        Ok(LiveContractState::default()),
        fail("cleanup failed"),
        move |_| {
            built_for_closure.store(true, Ordering::SeqCst);
            LiveValidationEvidence {
                outcome: "success".to_owned(),
                service_uri: "http://127.0.0.1:55051".to_owned(),
                ytmusic_addr: "http://127.0.0.1:50051".to_owned(),
                test_video_id: "video".to_owned(),
                expected_track_duration_ms: 180_000,
                active_validation_duration_after_resume_ms: 150_000,
                pause_silence_packet_count: 5,
                pause_silence_spacing_ms: vec![20, 20, 20, 20],
                live_staging_profile: "constrained-github-hosted".to_owned(),
                live_staging_service_cpus: "1.0".to_owned(),
                live_staging_cpu_contention_workers: 2,
                live_staging_http_read_delay_ms: 5,
                live_staging_http_read_jitter_ms: 25,
                validated_join_voice: false,
                validated_update_voice_context: false,
                validated_play: false,
                validated_pause: false,
                validated_resume: false,
                validated_invalid_resume_ignored: false,
                validated_redundant_pause_ignored: false,
                observer_proved_pause: false,
                observer_proved_resume: false,
                observer_pause_self_mute_observed: false,
                observer_pause_speaking_stopped: false,
                observer_pause_rtp_silence_observed: false,
                observer_resume_speaking_started: false,
                observer_pause_silence_ms: 0,
                observer_resume_packet_count: 0,
                validated_reconnect_rollover_during_playback: false,
                validated_stop: false,
                validated_stop_during_playback: false,
                validated_leave_voice: false,
                validated_leave_voice_during_playback: false,
                validated_get_state: false,
                validated_get_playback_metrics: false,
                validated_subscribe_events: false,
                saw_voice_connecting: false,
                saw_voice_ready: false,
                saw_track_resolving: false,
                saw_playing: false,
                saw_track_ended: true,
                observed_packet_count: 0,
                decoded_audio_ms: 0,
                observer_wall_clock_elapsed_ms: 0,
                observer_decoded_audio_to_wall_clock_ratio_ppm: 0,
                non_silent_audio_ms: 0,
                observer_rtp_inter_arrival: PlaybackDurationStatsEvidence::default(),
                observer_rtp_gap_count_gte_100ms: 0,
                observer_rtp_fast_interval_count: 0,
                observer_rtp_fast_interval_min_ms: 0,
                observer_rtp_fast_interval_min_us: 0,
                observer_decoded_audio_tempo_window_count: 0,
                observer_decoded_audio_tempo_window_post_source_buffer_count: 0,
                observer_decoded_audio_tempo_window_min_ratio_ppm: 0,
                observer_decoded_audio_tempo_window_max_ratio_ppm: 0,
                observer_decoded_audio_tempo_window_fast_count: 0,
                observer_decoded_audio_tempo_window_fastest_ratio_ppm: 0,
                observer_decoded_audio_tempo_window_fastest_media_ms: 0,
                observer_decoded_audio_tempo_window_fastest_wall_clock_us: 0,
                observer_decoded_audio_tempo_window_slow_count: 0,
                observer_decoded_audio_tempo_window_slowest_ratio_ppm: 0,
                observer_decoded_audio_tempo_window_slowest_media_ms: 0,
                observer_decoded_audio_tempo_window_slowest_wall_clock_us: 0,
                dave_transition_count_during_playback: 0,
                playback_metrics: None,
                reconnect_probe_metrics: None,
                validated_constrained_profile: false,
                validated_slow_jittery_http: false,
                failure_reason: None,
            }
        },
        move |_| {
            emitted_for_closure.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .expect("successful validation should emit evidence despite cleanup lag");

    assert!(built.load(Ordering::SeqCst));
    assert!(emitted.load(Ordering::SeqCst));
}

#[tokio::test]
async fn orchestration_waits_for_contract_and_play_completion() {
    let (contract_tx, contract_rx) = oneshot::channel::<Result<LiveContractState>>();
    let (play_tx, play_rx) = oneshot::channel::<Result<()>>();

    let mut orchestration = Box::pin(wait_for_play_and_live_contract(
        async move { contract_rx.await.expect("contract sender should complete") },
        async move { play_rx.await.expect("play sender should complete") },
    ));

    contract_tx
        .send(Ok(LiveContractState {
            saw_voice_ready: true,
            saw_playing: true,
            saw_track_ended: true,
            ..LiveContractState::default()
        }))
        .expect("contract result should be sent");

    timeout(TokioDuration::from_millis(25), &mut orchestration)
        .await
        .expect_err("orchestration must not finish until Play completes");

    play_tx.send(Ok(())).expect("play result should be sent");
    let state = orchestration
        .await
        .expect("orchestration should return once both futures succeed");
    assert!(state.saw_voice_ready);
    assert!(state.saw_playing);
    assert!(state.saw_track_ended);
}

#[tokio::test]
async fn orchestration_returns_play_error_without_observer_cleanup() {
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
async fn cleanup_treats_user_voice_state_404_as_absence() {
    let client = mock_http_client(404, r#"{"message":"Unknown Voice State","code":10065}"#).await;

    assert!(
        user_absent_from_guild_voice(&client, Id::new(2), Id::new(7))
            .await
            .unwrap(),
        "404 user voice state should confirm cleanup absence",
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
        ("OBSERVER_BOT_TOKEN".to_owned(), "observer-token".to_owned()),
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
        (
            "LIVE_STAGING_PROFILE".to_owned(),
            "constrained-github-hosted".to_owned(),
        ),
        ("LIVE_STAGING_SERVICE_CPUS".to_owned(), "1.0".to_owned()),
        (
            "LIVE_STAGING_CPU_CONTENTION_WORKERS".to_owned(),
            "2".to_owned(),
        ),
        ("LIVE_STAGING_HTTP_READ_DELAY_MS".to_owned(), "5".to_owned()),
        (
            "LIVE_STAGING_HTTP_READ_JITTER_MS".to_owned(),
            "25".to_owned(),
        ),
    ])
}

fn sample_playback_metrics() -> PlaybackStabilityEvidence {
    PlaybackStabilityEvidence {
        playback_epoch: 7,
        video_id: Some("video".to_owned()),
        selected_itag: Some(250),
        track_packet_count: 144,
        continuity_silence_packet_count: 0,
        inserted_silence_duration_ms: 0,
        track_interval: playback_stats(143, 20, 22, 25, 19, 28),
        track_media_duration_sent_ms: 2880,
        track_wall_clock_elapsed_ms: 2880,
        track_media_to_wall_clock_ratio_ppm: 1_000_000,
        track_fast_interval_count: 0,
        track_fast_interval_min_ms: 0,
        track_fast_interval_min_us: 0,
        track_tempo_window_count: 95,
        track_tempo_window_post_source_buffer_count: 1,
        track_tempo_window_min_ratio_ppm: 1_000_000,
        track_tempo_window_max_ratio_ppm: 1_000_000,
        track_tempo_window_fast_count: 0,
        track_tempo_window_fastest_ratio_ppm: 0,
        track_tempo_window_fastest_media_ms: 0,
        track_tempo_window_fastest_wall_clock_us: 0,
        track_tempo_window_slow_count: 0,
        track_tempo_window_slowest_ratio_ppm: 0,
        track_tempo_window_slowest_media_ms: 0,
        track_tempo_window_slowest_wall_clock_us: 0,
        skipped_source_frame_count: 0,
        skipped_source_duration_ms: 0,
        tempo_rebase_count: 0,
        all_packet_interval: playback_stats(143, 20, 22, 25, 19, 28),
        sender_lateness: playback_stats(144, 0, 2, 4, 0, 5),
        max_consecutive_late_packets: 2,
        current_consecutive_late_packets: 0,
        current_buffer_depth: playback_depth(4, 2048, 80, 3840),
        min_buffer_depth: playback_depth(4, 2048, 80, 3840),
        max_buffer_depth: playback_depth(12, 6144, 240, 11520),
        current_source_buffer_depth: playback_depth(240, 122880, 4800, 230400),
        min_source_buffer_depth: playback_depth(50, 25600, 1000, 48000),
        max_source_buffer_depth: playback_depth(250, 128000, 5000, 240000),
        source_buffer_depth: Some(source_buffer_depth_stats()),
        current_playout_buffer_depth: playback_depth(19, 9728, 380, 18_240),
        min_playout_buffer_depth: playback_depth(0, 0, 0, 0),
        max_playout_buffer_depth: playback_depth(20, 10240, 400, 19_200),
        egress_buffer_target_ms: 400,
        current_egress_buffer_depth: playback_depth(19, 9728, 380, 18_240),
        min_egress_buffer_depth: playback_depth(0, 0, 0, 0),
        max_egress_buffer_depth: playback_depth(20, 10240, 400, 19_200),
        prepared_rtp_queue_depth_ms: 380,
        prepared_track_queue_target_ms: 400,
        prepared_track_queue_low_watermark_ms: 300,
        prepared_track_queue_high_watermark_ms: 500,
        active_pre_pause_prepared_track_queue_depth: Some(playback_queue_depth_stats()),
        active_post_resume_prepared_track_queue_depth: Some(playback_queue_depth_stats()),
        prepared_track_queue_depth_sample_count: 100,
        prepared_track_queue_empty_count: 0,
        raw_send_event_count: 0,
        raw_prepared_track_queue_sample_count: 0,
        raw_prepared_playout_queue_event_count: 0,
        raw_send_events: Vec::new(),
        raw_prepared_track_queue_samples: Vec::new(),
        raw_prepared_playout_queue_events: Vec::new(),
        current_scheduled_silence_queue_depth: playback_depth(0, 0, 0, 0),
        max_scheduled_silence_queue_depth: playback_depth(0, 0, 0, 0),
        current_boundary_queue_depth: playback_depth(0, 0, 0, 0),
        max_boundary_queue_depth: playback_depth(1, 3, 20, 960),
        prepared_track_packet_drop_count: 0,
        prepared_silence_packet_drop_count: 0,
        prepared_packet_rebuild_count: 0,
        scheduled_silence_packet_count: 0,
        pause_media_boundary_count: 1,
        stop_media_boundary_count: 0,
        recovery_media_boundary_count: 0,
        natural_end_media_boundary_count: 0,
        dave_transition_recovery_reached_builder_count: 0,
        dave_transition_recovery_reached_deadline_sender_count: 0,
        source_underrun_reached_builder_count: 0,
        source_underrun_reached_deadline_sender_count: 0,
        discarded_source_frame_count: 0,
        discarded_source_duration_ms: 0,
        stop_discarded_source_frame_count: 0,
        stop_discarded_source_duration_ms: 0,
        interruption_discarded_source_frame_count: 0,
        interruption_discarded_source_duration_ms: 0,
        restored_source_frame_count: 0,
        restored_source_duration_ms: 0,
        source_buffer_target_ms: 5000,
        adaptive_buffer_target_ms: 5000,
        max_adaptive_buffer_target_ms: 5000,
        buffer_low_watermark_count: 0,
        source_buffer_low_watermark_count: 0,
        playout_buffer_low_watermark_count: 0,
        buffer_underrun_count: 0,
        playout_underrun_count: 0,
        egress_underrun_count: 0,
        egress_inserted_silence_duration_ms: 0,
        egress_dropped_music_frame_count: 0,
        egress_dropped_music_duration_ms: 0,
        source_underrun_count: 0,
        rebuffer_count: 0,
        refill_duration: playback_stats(3, 4, 6, 7, 2, 7),
        source_producer_fill_duration: playback_stats(3, 4, 6, 7, 2, 7),
        producer_stall_duration: playback_stats(3, 4, 6, 7, 2, 7),
        max_producer_lag_ms: 12,
        http_retry_count: 0,
        response_open_count: 1,
        range_reopen_count: 0,
        read_error_reopen_count: 0,
        url_reresolve_count: 0,
        pause_resume_first_intervals_ms: vec![20, 21],
        post_stall_first_intervals_ms: Vec::new(),
        post_rebuffer_first_intervals_ms: Vec::new(),
        playout_sender_lateness: playback_stats(144, 0, 2, 4, 0, 5),
        playout_builder_prepare_duration: playback_stats(144, 0, 1, 1, 0, 1),
        sender_send_duration: playback_stats(144, 0, 1, 2, 0, 2),
        sender_loop_non_send_work_duration: playback_stats(144, 0, 1, 1, 0, 1),
        max_consecutive_playout_late_packets: 2,
        max_consecutive_late_egress_ticks: 2,
        speaking_prepare_duration: playback_stats(1, 100, 100, 100, 100, 100),
        sender_forbidden_work_count: 0,
        gateway_event_drain_duration: playback_stats(144, 0, 1, 2, 0, 2),
        gateway_event_drain_count: 0,
        dave_transition_count: 0,
        dave_transition_count_during_playback: 0,
        stale_dave_send_prevented_count: 0,
        controlled_media_interruption_count: 0,
        media_clock_reset_count: 0,
        egress_clock_reset_count: 0,
        scheduler_late_reset_count: 0,
        source_underrun_reset_count: 0,
        pause_resume_reset_count: 0,
        dave_transition_recovery_reset_count: 0,
        gateway_interruptions: 0,
        dave_interruptions: 0,
        reconnect_interruptions: 0,
        ended: true,
    }
}

fn sample_reconnect_probe_metrics() -> PlaybackStabilityEvidence {
    PlaybackStabilityEvidence {
        playback_epoch: 8,
        track_packet_count: 32,
        reconnect_interruptions: 1,
        ended: false,
        ..sample_playback_metrics()
    }
}

fn playback_stats(
    samples: u64,
    p50_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    min_ms: u64,
    max_ms: u64,
) -> PlaybackDurationStatsEvidence {
    PlaybackDurationStatsEvidence {
        samples,
        p50_ms,
        p95_ms,
        p99_ms,
        min_ms,
        max_ms,
    }
}

fn playback_depth(
    packets: u64,
    bytes: u64,
    duration_ms: u64,
    duration_samples: u64,
) -> PlaybackBufferDepthEvidence {
    PlaybackBufferDepthEvidence {
        packets,
        bytes,
        duration_ms,
        duration_samples,
    }
}

fn playback_queue_depth_stats() -> PlaybackQueueDepthStatsEvidence {
    PlaybackQueueDepthStatsEvidence {
        sample_count: 50,
        empty_count: 0,
        current_depth: playback_depth(20, 10_240, 400, 19_200),
        min_depth: playback_depth(16, 8_192, 320, 15_360),
        p5_depth: playback_depth(16, 8_192, 320, 15_360),
        p50_depth: playback_depth(20, 10_240, 400, 19_200),
        p95_depth: playback_depth(24, 12_288, 480, 23_040),
        max_depth: playback_depth(25, 12_800, 500, 24_000),
    }
}

fn source_buffer_depth_stats() -> PlaybackQueueDepthStatsEvidence {
    PlaybackQueueDepthStatsEvidence {
        sample_count: 50,
        empty_count: 0,
        current_depth: playback_depth(240, 122_880, 4_800, 230_400),
        min_depth: playback_depth(50, 25_600, 1_000, 48_000),
        p5_depth: playback_depth(200, 102_400, 4_000, 192_000),
        p50_depth: playback_depth(240, 122_880, 4_800, 230_400),
        p95_depth: playback_depth(250, 128_000, 5_000, 240_000),
        max_depth: playback_depth(250, 128_000, 5_000, 240_000),
    }
}

fn event(kind: SessionEventKind, current_video_id: Option<&str>) -> SessionEvent {
    SessionEvent {
        kind,
        current_video_id: current_video_id.map(str::to_owned),
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
