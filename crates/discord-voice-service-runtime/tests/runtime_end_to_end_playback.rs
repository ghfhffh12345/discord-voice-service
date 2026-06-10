use std::{
    hint,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Instant as StdInstant,
};

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service_proto::discordvoice::v1::{
    SessionEvent, SessionEventKind, SubscribeEventsRequest,
};
use discord_voice_service_runtime::{
    Command, ControlService, PlaybackStabilitySnapshot, Readiness, SessionState, Supervisor,
};
use discord_voice_service_test_support::fake_discord::{FakeDiscordPeer, ObservedAudioPacket};
use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;
use discord_voice_service_test_support::fixtures::{
    spawn_stream_server, spawn_stream_server_with_chunk_jitter,
};
use discord_voice_service_voice::test_support::parse_rtp_header;
use futures::StreamExt;
use tokio::time::{Duration, Instant};
use tonic::Request;

const SERVICE_USER_ID: &str = "1111111111111111";
const LATE_LISTENER_USER_ID: &str = "7777777777777777";
const TRACK_FRAME_DURATION: Duration = Duration::from_millis(20);
const MIN_TRACK_SEND_START_INTERVAL: Duration = Duration::from_millis(20);
const MIN_FAKE_UDP_OBSERVED_INTERVAL: Duration = Duration::from_millis(18);
const STOP_SPEAKING_GATEWAY_REPEAT_COUNT: usize = 3;
const MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM: u64 = 980_000;
const MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM: u64 = 1_020_000;
const DISCORD_EGRESS_BUFFER_TARGET_MS: u64 = 400;
const DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS: u64 = 500;
#[tokio::test]
async fn runtime_end_to_end_playback_join_voice_then_play_reaches_connected_runtime_playback_path()
{
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[3].current_video_id, "video-1");
    assert_eq!(startup_events[3].selected_itag, 250);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);
    assert_eq!(startup_events[4].current_video_id, "video-1");
    assert_eq!(startup_events[4].selected_itag, 250);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");

    let ending_events = collect_events(&mut stream, 1).await;
    assert_eq!(ending_events[0].kind, SessionEventKind::TrackEnded as i32);
    assert_eq!(ending_events[0].current_video_id, "video-1");
    assert_eq!(ending_events[0].selected_itag, 250);

    assert!(fake_voice.non_silence_audio_frame_count_at_least(5).await >= 5);
    let first_five_span = fake_voice.non_silence_audio_frame_span_for_first(5).await;
    assert!(
        first_five_span >= Duration::from_millis(70),
        "the first five audio frames should span at least four 20ms pacing intervals: {first_five_span:?}"
    );
    assert_eq!(fake_voice.discovery_count().await, 1);

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.position_ms, 0);

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert!(metrics.ended);
    assert!(metrics.track_packet_count >= 5);
    assert_eq!(metrics.buffer_underrun_count, 0);
    assert_eq!(metrics.skipped_source_frame_count, 0);
    assert_eq!(metrics.skipped_source_duration_ms, 0);
    assert_eq!(metrics.continuity_silence_packet_count, 0);
    assert_eq!(metrics.inserted_silence_duration_ms, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_play_prebuffers_five_seconds_before_playing() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-long.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let pre_playing_events = collect_events(&mut stream, 4).await;
    assert_eq!(
        pre_playing_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(
        pre_playing_events[1].kind,
        SessionEventKind::VoiceReady as i32
    );
    assert_eq!(
        pre_playing_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(
        pre_playing_events[3].kind,
        SessionEventKind::Buffering as i32
    );
    assert_eq!(
        fake_voice.non_silence_audio_frame_count().await,
        0,
        "no track frames should be sent before Playing is emitted"
    );

    let playing_events = collect_events(&mut stream, 1).await;
    assert_eq!(playing_events[0].kind, SessionEventKind::Playing as i32);

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert_eq!(metrics.source_buffer_target_ms, 5_000);
    assert!(
        metrics.max_source_buffer_depth.duration_ms >= 5_000,
        "Playing must follow the five-second source prebuffer: {metrics:?}"
    );
    assert_raw_egress_metrics_published(&metrics, "Playing prebuffer");
    assert_eq!(metrics.continuity_silence_packet_count, 0);
    assert_eq!(metrics.buffer_underrun_count, 0);
    assert_eq!(metrics.playout_underrun_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_short_track_prebuffers_to_end_of_stream() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let events = collect_events(&mut stream, 6).await;
    assert_eq!(events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(events[4].kind, SessionEventKind::Playing as i32);
    assert_eq!(events[5].kind, SessionEventKind::TrackEnded as i32);

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert!(metrics.ended);
    assert_eq!(metrics.source_buffer_target_ms, 5_000);
    assert!(
        metrics.max_source_buffer_depth.duration_ms < 5_000,
        "short fixture should start after buffering its complete stream, not after reaching five seconds: {metrics:?}"
    );
    assert!(
        metrics.max_source_buffer_depth.packets >= metrics.track_packet_count,
        "all short-track packets should be source-buffered before playback starts: {metrics:?}"
    );
    assert_eq!(metrics.continuity_silence_packet_count, 0);
    assert_eq!(metrics.buffer_underrun_count, 0);
    assert_eq!(metrics.playout_underrun_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_packets_hold_near_20ms_cadence_without_refill_stalls() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-long.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);
    let requests_at_playing = http.request_count().await;

    let timestamps = fake_voice.non_silence_audio_frame_times_at_least(50).await;
    let requests_after_50_packets = http.request_count().await;
    let intervals = intervals_between(&timestamps[..50]);
    let stats = interval_stats(&intervals);
    eprintln!(
        "normal playback cadence: {stats:?}; requests_at_playing={requests_at_playing}; requests_after_50_packets={requests_after_50_packets}"
    );
    assert_eq!(
        requests_at_playing, requests_after_50_packets,
        "successful full-response playback should not perform a mid-playback HTTP refill during the observed packet window"
    );

    assert!(
        stats.p50 >= Duration::from_millis(19),
        "normal playback median interval must reject the known sub-19ms cadence: {stats:?}"
    );
    assert!(
        stats.p50 <= Duration::from_millis(22),
        "normal playback median interval should not drift to 21-22ms: {stats:?}"
    );
    assert_fake_udp_observed_intervals_not_bursty(&stats, "normal playback");
    assert!(
        stats.p95 <= Duration::from_millis(22),
        "normal playback p95 should stay near 20ms on real OS time: {stats:?}"
    );
    assert!(
        stats.p99 <= Duration::from_millis(50),
        "normal playback p99 should stay bounded without refill stalls: {stats:?}"
    );
    assert!(
        stats.max < Duration::from_millis(100),
        "normal playback must not have a perceptible >=100ms interval: {stats:?}"
    );
    let observed_wall_clock =
        timestamps[49].saturating_duration_since(timestamps[0]) + TRACK_FRAME_DURATION;
    assert!(
        observed_wall_clock >= Duration::from_millis(980),
        "50 consecutive 20ms packets must occupy about one second of wall time: wall={observed_wall_clock:?}; stats={stats:?}"
    );
    assert!(
        observed_wall_clock <= Duration::from_millis(1_060),
        "fake UDP receive timestamps include real scheduler noise; internal send-start tempo metrics enforce the 1.02x slow bound: wall={observed_wall_clock:?}; stats={stats:?}"
    );

    let packets = fake_voice.audio_packets_at_least(50).await;
    let headers = packets
        .iter()
        .take(50)
        .map(|packet| parse_rtp_header(packet).unwrap())
        .collect::<Vec<_>>();
    let timestamp_deltas = headers
        .windows(2)
        .map(|window| window[1].timestamp.wrapping_sub(window[0].timestamp))
        .collect::<Vec<_>>();
    assert!(
        timestamp_deltas.iter().all(|delta| *delta == 960),
        "20ms fixture packets should advance RTP timestamps by 960 samples: {timestamp_deltas:?}"
    );

    let post_reservoir_timestamps =
        audio_frame_times_at_least_with_timeout(&fake_voice, 310, Duration::from_secs(8)).await;
    assert!(
        post_reservoir_timestamps.len() >= 310,
        "post-source-reservoir playback should dispatch at least 310 track packets; observed {}",
        post_reservoir_timestamps.len()
    );
    let post_reservoir_intervals = intervals_between(&post_reservoir_timestamps[250..310]);
    let post_reservoir_stats = interval_stats(&post_reservoir_intervals);
    let post_reservoir_wall_clock = post_reservoir_timestamps[309]
        .saturating_duration_since(post_reservoir_timestamps[250])
        + TRACK_FRAME_DURATION;
    assert!(
        post_reservoir_stats.p50 >= Duration::from_millis(19)
            && post_reservoir_stats.p50 <= Duration::from_millis(22),
        "post-source-reservoir p50 should stay near cadence: wall={post_reservoir_wall_clock:?}; stats={post_reservoir_stats:?}; intervals={post_reservoir_intervals:?}"
    );
    assert!(
        post_reservoir_stats.p05 >= MIN_FAKE_UDP_OBSERVED_INTERVAL,
        "post-source-reservoir fake UDP p05 should reject burst catch-up while allowing receive timestamp jitter: wall={post_reservoir_wall_clock:?}; stats={post_reservoir_stats:?}; intervals={post_reservoir_intervals:?}"
    );
    assert!(
        post_reservoir_stats.p95 <= Duration::from_millis(23),
        "post-source-reservoir p95 should stay bounded on real OS time: wall={post_reservoir_wall_clock:?}; stats={post_reservoir_stats:?}; intervals={post_reservoir_intervals:?}"
    );

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    eprintln!("runtime playback metrics: {metrics:?}");
    assert!(metrics.track_packet_count >= 50);
    assert!(metrics.track_interval.samples >= 49);
    assert!(
        metrics.track_media_duration_sent_ms >= 1_000,
        "runtime metrics should report sent track media duration: {metrics:?}"
    );
    assert!(
        metrics.track_wall_clock_elapsed_ms >= 980,
        "runtime metrics should report near-real-time wall-clock duration: {metrics:?}"
    );
    assert_track_tempo_metrics_within_bounds(&metrics, "normal playback");
    assert!(
        metrics.track_tempo_window_post_source_buffer_count > 0,
        "runtime metrics should include rolling tempo windows after the 5000ms source reservoir: {metrics:?}"
    );
    assert!(
        metrics.track_interval.p95_ms <= 45,
        "runtime metrics p95 should stay near cadence: {metrics:?}"
    );
    assert!(
        metrics.track_interval.p99_ms <= 70,
        "runtime metrics p99 should stay bounded: {metrics:?}"
    );
    assert!(
        metrics.track_interval.p99_ms <= 50,
        "normal runtime metrics p99 should stay within the normal cadence budget: {metrics:?}"
    );
    assert!(
        metrics.track_interval.max_ms < 100,
        "normal runtime metrics must not show a >=100ms interval: {metrics:?}"
    );
    assert_eq!(
        metrics.skipped_source_frame_count, 0,
        "steady playback stopped after sustained observation must not report skipped source frames: {metrics:?}"
    );
    assert_eq!(metrics.skipped_source_duration_ms, 0);
    assert_eq!(
        metrics.buffer_underrun_count, 0,
        "normal playback should not underrun: {metrics:?}"
    );
    assert_eq!(
        metrics.playout_underrun_count, 0,
        "normal playback should not report prepared RTP underruns: {metrics:?}"
    );
    assert_eq!(metrics.source_buffer_target_ms, 5_000);
    assert!(
        metrics.max_source_buffer_depth.duration_ms >= 5_000,
        "normal playback should prebuffer the source reservoir: {metrics:?}"
    );
    assert!(
        metrics.max_source_buffer_depth.duration_ms > metrics.max_buffer_depth.duration_ms,
        "source reservoir depth should be reported separately from sender depth: {metrics:?}"
    );
    assert_bounded_raw_egress_metrics(&metrics, "normal playback");
    assert!(metrics.source_producer_fill_duration.samples >= 1);
    assert!(metrics.playout_builder_prepare_duration.samples > 0);
    assert!(metrics.sender_send_duration.samples >= 50);
    assert!(
        metrics.sender_loop_non_send_work_duration.max_ms <= 2,
        "normal sender non-send work should stay near zero: {metrics:?}"
    );
    assert_eq!(metrics.source_underrun_count, 0);
    assert_eq!(metrics.sender_forbidden_work_count, 0);
    assert!(metrics.playout_sender_lateness.samples >= 50);
    assert!(metrics.max_consecutive_playout_late_packets <= 2);
    assert_eq!(metrics.adaptive_buffer_target_ms, 5_000);
    assert_eq!(metrics.max_adaptive_buffer_target_ms, 5_000);
    assert_eq!(metrics.rebuffer_count, 0);
    assert!(metrics.refill_duration.samples >= 1);
    assert_eq!(metrics.http_retry_count, 0);
    assert_eq!(metrics.range_reopen_count, 0);
    assert_eq!(metrics.url_reresolve_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_does_not_burst_under_jittery_http_and_cpu_contention() {
    let _cpu_contention = CpuContentionGuard::start(2);
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server_with_chunk_jitter(
        "audio-long.webm",
        8 * 1024,
        Duration::from_millis(2),
        Duration::from_millis(4 * 2),
    )
    .await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events_with_timeout(&mut stream, 5, Duration::from_secs(8)).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);

    let timestamps = fake_voice.non_silence_audio_frame_times_at_least(80).await;
    let intervals = intervals_between(&timestamps[..80]);
    let stats = interval_stats(&intervals);
    eprintln!("stress playback cadence: {stats:?}");

    assert!(
        stats.p95 <= Duration::from_millis(45),
        "stress playback p95 should stay bounded under CPU and HTTP jitter: {stats:?}"
    );
    assert!(
        stats.p99 <= Duration::from_millis(70),
        "stress playback p99 should stay bounded under CPU and HTTP jitter: {stats:?}"
    );
    assert!(
        stats.max < Duration::from_millis(100),
        "stress playback must not have a perceptible >=100ms interval: {stats:?}"
    );

    let packets = fake_voice.audio_packets_at_least(80).await;
    let headers = packets
        .iter()
        .take(80)
        .map(|packet| parse_rtp_header(packet).unwrap())
        .collect::<Vec<_>>();
    let timestamp_deltas = headers
        .windows(2)
        .map(|window| window[1].timestamp.wrapping_sub(window[0].timestamp))
        .collect::<Vec<_>>();
    assert!(
        timestamp_deltas.iter().all(|delta| *delta == 960),
        "stress playback should keep 20ms RTP timestamp increments: {timestamp_deltas:?}"
    );

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    eprintln!("stress playback metrics: {metrics:?}");
    assert!(metrics.track_packet_count >= 80);
    assert_track_tempo_metrics_within_bounds(&metrics, "stress playback");
    assert!(
        metrics.track_interval.p95_ms <= 45,
        "stress metrics p95 should stay bounded: {metrics:?}"
    );
    assert!(
        metrics.track_interval.p99_ms <= 70,
        "stress metrics p99 should stay bounded: {metrics:?}"
    );
    assert!(
        metrics.track_interval.max_ms < 100,
        "stress metrics must not show a >=100ms interval: {metrics:?}"
    );
    assert_eq!(
        metrics.buffer_underrun_count, 0,
        "stress playback should keep compressed audio buffered before stop: {metrics:?}"
    );
    assert_eq!(
        metrics.skipped_source_frame_count, 0,
        "steady stress playback stopped after observation must not report skipped source frames: {metrics:?}"
    );
    assert_eq!(metrics.skipped_source_duration_ms, 0);
    assert_eq!(
        metrics.playout_underrun_count, 0,
        "stress playback should not report prepared RTP underruns: {metrics:?}"
    );
    assert!(metrics.refill_duration.samples >= 1);
    assert!(metrics.sender_lateness.samples >= 80);
    assert_eq!(metrics.source_buffer_target_ms, 5_000);
    assert!(
        metrics.max_source_buffer_depth.duration_ms >= 5_000,
        "stress playback should prebuffer the source reservoir: {metrics:?}"
    );
    assert!(
        metrics.max_source_buffer_depth.duration_ms > metrics.max_buffer_depth.duration_ms,
        "stress playback should report source reservoir depth separately: {metrics:?}"
    );
    assert_bounded_raw_egress_metrics(&metrics, "stress playback");
    assert!(metrics.source_producer_fill_duration.samples >= 1);
    assert!(metrics.playout_builder_prepare_duration.samples > 0);
    assert!(metrics.sender_send_duration.samples >= 80);
    assert!(
        metrics.sender_loop_non_send_work_duration.max_ms <= 2,
        "stress sender non-send work should stay near zero: {metrics:?}"
    );
    assert_eq!(metrics.source_underrun_count, 0);
    assert_eq!(metrics.sender_forbidden_work_count, 0);
    assert!(metrics.playout_sender_lateness.samples >= 80);
    assert!(metrics.max_consecutive_playout_late_packets <= 2);
    assert!(
        metrics.max_producer_lag_ms <= 1_200,
        "producer should refill in bounded batches instead of hoarding buffered Opus: {metrics:?}"
    );
    assert_eq!(metrics.adaptive_buffer_target_ms, 5_000);
    assert_eq!(metrics.max_adaptive_buffer_target_ms, 5_000);
    assert_eq!(metrics.rebuffer_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_alternating_two_ms_send_delay_keeps_twenty_ms_output_cadence()
{
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-long.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    supervisor.set_live_media_send_delay_for_tests(|packet_index| {
        if packet_index < 70 && packet_index % 2 == 0 {
            Some(Duration::from_millis(2))
        } else {
            None
        }
    });
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);

    let timestamps = fake_voice.non_silence_audio_frame_times_at_least(60).await;
    let intervals = intervals_between(&timestamps[..60]);
    let stats = interval_stats(&intervals);
    eprintln!("alternating delayed-send cadence: {stats:?}");
    assert!(
        stats.p95 <= Duration::from_millis(35),
        "alternating 2ms send-path delay should remain bounded while preserving tempo: {stats:?}"
    );
    assert!(
        stats.max < Duration::from_millis(100),
        "alternating 2ms send-path delay must not create perceptible gaps: {stats:?}"
    );

    supervisor.send(Command::Stop).await.unwrap();
    supervisor.clear_live_media_send_delay_for_tests();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert_eq!(
        metrics.track_fast_interval_count, 0,
        "alternating 2ms post-boundary send delay must not create shortened send-start intervals: {metrics:?}"
    );
    assert_eq!(metrics.track_fast_interval_min_us, 0);
    assert_eq!(
        metrics.track_tempo_window_fast_count, 0,
        "alternating 2ms post-boundary send delay must not create faster-than-real-time windows: {metrics:?}"
    );
    assert!(
        metrics.track_tempo_window_max_ratio_ppm <= MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM,
        "alternating 2ms post-boundary send delay must not make media faster than wall clock: {metrics:?}"
    );
    assert_eq!(
        metrics.tempo_rebase_count, 0,
        "ordinary 2ms injected send delay must not rebase the media clock: {metrics:?}"
    );
    assert_eq!(metrics.scheduler_late_reset_count, 0);
    assert_eq!(metrics.controlled_media_interruption_count, 0);
    assert_eq!(metrics.source_underrun_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_media_driver_delay_does_not_perturb_deadline_sender() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-long.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let delay_used = Arc::new(AtomicBool::new(false));
    let delay_used_for_hook = Arc::clone(&delay_used);
    supervisor.set_live_media_send_delay_for_tests(move |packet_index| {
        if packet_index == 20 && !delay_used_for_hook.swap(true, Ordering::SeqCst) {
            Some(Duration::from_millis(300))
        } else {
            None
        }
    });
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);
    assert!(fake_voice.speaking_state_count_at_least(1, 3).await >= 3);

    let timestamps = fake_voice.non_silence_audio_frame_times_at_least(45).await;
    assert!(
        delay_used.load(Ordering::SeqCst),
        "test hook should have delayed the live media driver"
    );
    let intervals = intervals_between(&timestamps[..45]);
    let stats = interval_stats(&intervals);
    let delayed_interval = intervals[19];
    let post_delay_interval = intervals[20];
    assert!(
        delayed_interval <= Duration::from_millis(30),
        "driver-side delay must not create a late RTP sender tick: {delayed_interval:?}; stats={stats:?}"
    );
    assert!(
        post_delay_interval >= MIN_FAKE_UDP_OBSERVED_INTERVAL,
        "driver-side delay recovery must not catch up with a burst: {post_delay_interval:?}; stats={stats:?}"
    );
    assert!(
        post_delay_interval <= Duration::from_millis(30),
        "driver-side delay should leave sender cadence near 20ms: {post_delay_interval:?}; stats={stats:?}"
    );
    assert!(
        stats.p95 <= Duration::from_millis(22),
        "driver-side delay must be absorbed by the prepared sender reservoir: {stats:?}"
    );
    assert_no_fake_udp_interval_bursty(&intervals, "driver-side delay recovery");

    supervisor.send(Command::Stop).await.unwrap();
    supervisor.clear_live_media_send_delay_for_tests();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert!(
        metrics.playout_sender_lateness.max_ms <= 5,
        "driver-side delay must not appear as RTP sender lateness: {metrics:?}"
    );
    assert!(
        metrics.track_media_to_wall_clock_ratio_ppm >= MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM,
        "driver-side delay must not slow heard media below real time: {metrics:?}"
    );
    assert!(
        metrics.track_media_to_wall_clock_ratio_ppm <= MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM,
        "driver-side delay must not run media faster than wall clock: {metrics:?}"
    );
    assert_eq!(
        metrics.track_fast_interval_count, 0,
        "late recovery send-start metrics must not show burst intervals: {metrics:?}"
    );
    assert_eq!(metrics.playout_underrun_count, 0);
    assert_eq!(metrics.source_underrun_count, 0);
    assert_eq!(metrics.sender_forbidden_work_count, 0);
    assert_bounded_raw_egress_metrics(&metrics, "driver-side delay recovery");
    assert!(metrics.playout_builder_prepare_duration.samples > 0);
    assert_eq!(metrics.scheduler_late_reset_count, 0);
    assert_eq!(metrics.media_clock_reset_count, 0);
    assert_eq!(metrics.controlled_media_interruption_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_pause_stops_audio_until_resume_without_bursting() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);

    assert!(fake_voice.audio_frame_count_at_least(4).await >= 4);
    assert!(fake_voice.speaking_state_count_at_least(1, 1).await >= 1);

    let frames_before_invalid_resume = fake_voice.audio_frame_count().await;
    supervisor.send(Command::Resume).await.unwrap();
    tokio::time::timeout(Duration::from_millis(80), stream.next())
        .await
        .expect_err("Resume while already playing must not emit a playback event");
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "Resume while already playing must not reconnect voice media"
    );
    assert!(
        fake_voice
            .audio_frame_count_at_least(frames_before_invalid_resume + 2)
            .await
            >= frames_before_invalid_resume + 2,
        "Resume while already playing must leave playback running normally"
    );

    let packets_before_pause = fake_voice.observed_audio_packets().await;
    let speaking_zeros_before_pause = fake_voice.speaking_state_count(0).await;
    supervisor.send(Command::Pause).await.unwrap();
    let pause_events = collect_events(&mut stream, 1).await;
    assert_eq!(pause_events[0].kind, SessionEventKind::Paused as i32);
    assert!(
        fake_voice
            .speaking_state_count_at_least(0, speaking_zeros_before_pause + 1)
            .await
            > speaking_zeros_before_pause,
        "pause must send Speaking 0 after its silence tail"
    );
    assert!(
        fake_voice
            .speaking_state_count_at_least(
                0,
                speaking_zeros_before_pause + STOP_SPEAKING_GATEWAY_REPEAT_COUNT,
            )
            .await
            >= speaking_zeros_before_pause + STOP_SPEAKING_GATEWAY_REPEAT_COUNT,
        "pause should complete the stop-speaking gateway repeat sequence"
    );
    let packets_after_pause = fake_voice.observed_audio_packets().await;
    let silence_tail_start =
        find_five_packet_silence_tail(&packets_after_pause, packets_before_pause.len());
    assert_five_packet_silence_tail(&packets_after_pause, silence_tail_start);

    supervisor.send(Command::Pause).await.unwrap();
    tokio::time::timeout(Duration::from_millis(80), stream.next())
        .await
        .expect_err("redundant Pause while already paused must not emit a playback event");
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "redundant Pause while already paused must not reconnect voice media"
    );

    let paused_count = fake_voice.audio_frame_count().await;
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert_eq!(
        fake_voice.audio_frame_count().await,
        paused_count,
        "audio packets must stop while playback is paused"
    );

    supervisor
        .send(Command::UpdateVoiceContext {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-2", "token-2"),
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert_eq!(
        fake_voice.audio_frame_count().await,
        paused_count,
        "refreshing voice context while paused must not resume audio"
    );
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "refreshing voice context while paused must not reconnect voice media"
    );

    let speaking_ones_before_resume = fake_voice.speaking_state_count(1).await;
    let non_silence_frames_before_resume = fake_voice.non_silence_audio_frame_count().await;
    supervisor.send(Command::Resume).await.unwrap();
    let resume_events = collect_events(&mut stream, 1).await;
    assert_eq!(resume_events[0].kind, SessionEventKind::Playing as i32);
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "resume should not rebuild voice media for a transient pause"
    );
    assert!(
        fake_voice
            .speaking_state_count_at_least(1, speaking_ones_before_resume + 1)
            .await
            > speaking_ones_before_resume,
        "resume must send Speaking 1 before resumed media"
    );
    let speaking_one_times = fake_voice
        .speaking_state_times_at_least(1, speaking_ones_before_resume + 1)
        .await;
    let resumed_non_silence_times = fake_voice
        .non_silence_audio_frame_times_at_least(non_silence_frames_before_resume + 1)
        .await;
    assert!(
        speaking_one_times[speaking_ones_before_resume]
            <= resumed_non_silence_times[non_silence_frames_before_resume],
        "resume Speaking 1 must be observed before the first resumed non-silence RTP packet"
    );

    let resumed_target = paused_count + 4;
    assert!(fake_voice.audio_frame_count_at_least(resumed_target).await >= resumed_target);
    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();
    let stop_events = collect_events(&mut stream, 2).await;
    assert!(
        stop_events
            .iter()
            .any(|event| event.kind == SessionEventKind::Stopped as i32)
    );
}

#[tokio::test]
async fn runtime_end_to_end_playback_pause_resume_without_voice_context_refresh_keeps_voice_media_connected()
 {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);
    assert!(fake_voice.audio_frame_count_at_least(4).await >= 4);
    assert_eq!(fake_voice.discovery_count().await, 1);

    let packets_before_pause = fake_voice.observed_audio_packets().await;
    let speaking_zeros_before_pause = fake_voice.speaking_state_count(0).await;
    supervisor.send(Command::Pause).await.unwrap();
    let pause_events = collect_events(&mut stream, 1).await;
    assert_eq!(pause_events[0].kind, SessionEventKind::Paused as i32);
    assert!(
        fake_voice
            .speaking_state_count_at_least(0, speaking_zeros_before_pause + 1)
            .await
            > speaking_zeros_before_pause,
        "pause must send Speaking 0 after its silence tail"
    );
    assert!(
        fake_voice
            .speaking_state_count_at_least(
                0,
                speaking_zeros_before_pause + STOP_SPEAKING_GATEWAY_REPEAT_COUNT,
            )
            .await
            >= speaking_zeros_before_pause + STOP_SPEAKING_GATEWAY_REPEAT_COUNT,
        "pause should complete the stop-speaking gateway repeat sequence"
    );
    let packets_after_pause = fake_voice.observed_audio_packets().await;
    let silence_tail_start =
        find_five_packet_silence_tail(&packets_after_pause, packets_before_pause.len());
    assert_five_packet_silence_tail(&packets_after_pause, silence_tail_start);
    let silence_tail_end = silence_tail_start + 5;

    let paused_count = fake_voice.audio_frame_count().await;
    assert_eq!(
        paused_count, silence_tail_end,
        "pause should stop RTP immediately after the five-packet silence tail"
    );
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert_eq!(
        fake_voice.audio_frame_count().await,
        paused_count,
        "audio packets must stop while playback is paused"
    );
    let non_silence_frames_before_resume = fake_voice.non_silence_audio_frame_count().await;
    let speaking_ones_before_resume = fake_voice.speaking_state_count(1).await;

    supervisor.send(Command::Resume).await.unwrap();
    let resume_events = collect_events(&mut stream, 1).await;
    assert_eq!(resume_events[0].kind, SessionEventKind::Playing as i32);
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "resume without a paused voice context refresh must not reconnect voice media"
    );
    assert!(
        fake_voice
            .speaking_state_count_at_least(1, speaking_ones_before_resume + 1)
            .await
            > speaking_ones_before_resume,
        "resume without transport rebuild must still prepare a fresh Speaking 1"
    );
    let speaking_one_times = fake_voice
        .speaking_state_times_at_least(1, speaking_ones_before_resume + 1)
        .await;

    let resumed_target = paused_count + 4;
    assert!(fake_voice.audio_frame_count_at_least(resumed_target).await >= resumed_target);
    let resumed_non_silence_target = non_silence_frames_before_resume + 5;
    let resumed_non_silence_times = fake_voice
        .non_silence_audio_frame_times_at_least(resumed_non_silence_target)
        .await;
    let first_resumed_interval = resumed_non_silence_times[non_silence_frames_before_resume]
        .checked_duration_since(speaking_one_times[speaking_ones_before_resume])
        .unwrap_or(Duration::ZERO);
    assert!(
        first_resumed_interval <= Duration::from_millis(140),
        "first resumed non-silence frame should arrive promptly after resumed Speaking 1: {first_resumed_interval:?}"
    );
    assert!(
        speaking_one_times[speaking_ones_before_resume]
            <= resumed_non_silence_times[non_silence_frames_before_resume],
        "resume Speaking 1 must be observed before the first resumed non-silence RTP packet"
    );
    let packets_after_resume = fake_voice
        .observed_audio_packets_at_least(silence_tail_end + 1)
        .await;
    let first_resumed_packet_index = packets_after_resume
        .iter()
        .enumerate()
        .skip(silence_tail_end)
        .find_map(|(index, packet)| (!packet.is_stop_silence).then_some(index))
        .expect("resume should send a non-silence music packet after the pause tail");
    assert_eq!(
        first_resumed_packet_index, silence_tail_end,
        "resume must not send extra RTP packets before the first resumed music packet"
    );
    assert_rtp_packet_follows(
        &packets_after_resume[silence_tail_end - 1],
        &packets_after_resume[first_resumed_packet_index],
        "first resumed music packet",
    );
    let speaking_ones_after_first_resumed_frame = fake_voice.speaking_state_count(1).await;
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert_eq!(
        fake_voice.speaking_state_count(1).await,
        speaking_ones_after_first_resumed_frame,
        "resumed media packets must not restart Speaking 1 after Playing"
    );
    let resumed_intervals = intervals_between(
        &resumed_non_silence_times[non_silence_frames_before_resume..resumed_non_silence_target],
    );
    let resumed_stats = interval_stats(&resumed_intervals);
    eprintln!("pause/resume cadence: {resumed_stats:?}");
    assert_no_fake_udp_interval_bursty(&resumed_intervals, "pause/resume playback");
    assert!(
        resumed_stats.max <= Duration::from_millis(70),
        "resume should restart the pacer deadline before sending sustained audio: {resumed_stats:?}"
    );
    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert!(
        !metrics.pause_resume_first_intervals_ms.is_empty(),
        "resume should record post-resume intervals: {metrics:?}"
    );
    assert!(
        metrics
            .pause_resume_first_intervals_ms
            .iter()
            .all(|interval_ms| *interval_ms
                >= u64::try_from(MIN_TRACK_SEND_START_INTERVAL.as_millis()).unwrap()),
        "resume metrics should not show burst intervals: {metrics:?}"
    );
    assert_eq!(
        metrics.skipped_source_frame_count, 0,
        "pause/resume must restore any selected unsent music frame instead of reporting it skipped: {metrics:?}"
    );
    assert_eq!(metrics.skipped_source_duration_ms, 0);
    assert_eq!(
        metrics.scheduler_late_reset_count, 0,
        "pause/resume must not report the preserved frame as scheduler-late: {metrics:?}"
    );
    assert_eq!(
        metrics.controlled_media_interruption_count, 0,
        "pause/resume must stay out of controlled interruption accounting: {metrics:?}"
    );
    assert_eq!(metrics.buffer_underrun_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_reconnect_rollover_does_not_count_unsent_frame_as_skipped() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-long.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;
    let voice = fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1");

    supervisor
        .send(Command::JoinVoice {
            voice: voice.clone(),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);
    assert!(fake_voice.audio_frame_count_at_least(4).await >= 4);

    supervisor
        .send(Command::UpdateVoiceContext { voice })
        .await
        .unwrap();
    play_task.await.unwrap().unwrap();

    let rollover_events = collect_events_with_timeout(&mut stream, 5, Duration::from_secs(5)).await;
    assert!(
        rollover_events
            .iter()
            .any(|event| event.kind == SessionEventKind::VoiceReconnecting as i32),
        "rollover should emit VoiceReconnecting: {rollover_events:?}"
    );
    assert!(
        rollover_events
            .iter()
            .any(|event| event.kind == SessionEventKind::VoiceReady as i32),
        "rollover should emit VoiceReady: {rollover_events:?}"
    );
    assert!(
        rollover_events
            .iter()
            .any(|event| event.kind == SessionEventKind::Playing as i32),
        "rollover should resume playback: {rollover_events:?}"
    );

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("rollover probe should publish interrupted playback metrics");
    assert!(
        metrics.reconnect_interruptions > 0,
        "rollover metrics should count the reconnect interruption: {metrics:?}"
    );
    assert_eq!(
        metrics.skipped_source_frame_count, 0,
        "rollover must not report an unsent, replayable frame as skipped: {metrics:?}"
    );
    assert_eq!(metrics.skipped_source_duration_ms, 0);
    supervisor.send(Command::Stop).await.unwrap();
}

#[tokio::test]
async fn runtime_end_to_end_playback_join_voice_accepts_self_only_pending_initial_dave_session() {
    let fake_voice = FakeDiscordPeer::spawn_with_dave_self_only_no_proposals().await;
    let supervisor = Supervisor::new();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", SERVICE_USER_ID, "session-1", "token-1"),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 2).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(supervisor.snapshot().await.state, SessionState::VoiceReady);
    assert!(fake_voice.saw_dave_key_package_before_prepare_epoch().await);
    assert!(
        !fake_voice
            .saw_dave_commit_welcome_within(Duration::from_millis(100))
            .await
    );
}

#[tokio::test]
async fn runtime_end_to_end_playback_handles_queued_replayed_established_join_welcome_before_first_audio_frame()
 {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", SERVICE_USER_ID, "session-1", "token-1"),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 2).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);

    fake_voice
        .replay_established_join_welcome_transition()
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let playback_events = collect_events(&mut stream, 3).await;
    assert_eq!(
        playback_events[0].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(playback_events[1].kind, SessionEventKind::Buffering as i32);
    assert_eq!(playback_events[2].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(2).await >= 2);

    let ending_events = collect_events(&mut stream, 1).await;
    assert_eq!(ending_events[0].kind, SessionEventKind::TrackEnded as i32);
}

#[tokio::test]
async fn runtime_end_to_end_playback_handles_queued_replayed_local_init_creator_commit_before_first_audio_frame()
 {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice =
        FakeDiscordPeer::spawn_with_dave_queued_init_prepare_commit_until_control().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", SERVICE_USER_ID, "session-1", "token-1"),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 2).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);

    fake_voice
        .replay_local_init_creator_commit_transition()
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let playback_events = collect_events(&mut stream, 3).await;
    assert_eq!(
        playback_events[0].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(playback_events[1].kind, SessionEventKind::Buffering as i32);
    assert_eq!(playback_events[2].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(2).await >= 2);

    let ending_events = collect_events(&mut stream, 1).await;
    assert_eq!(ending_events[0].kind, SessionEventKind::TrackEnded as i32);
}

#[tokio::test]
async fn runtime_end_to_end_playback_dave_transition_during_playback_does_not_send_stale_audio() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-long.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", SERVICE_USER_ID, "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);

    assert!(fake_voice.audio_frame_count_at_least(3).await >= 3);
    let creator_decrypted = fake_voice
        .decrypt_last_dave_audio_frame_from_creator(SERVICE_USER_ID)
        .await
        .expect("established DAVE group member should decrypt pre-transition media");
    assert!(!creator_decrypted.is_empty());

    fake_voice
        .inject_late_dave_listener_transition_after_gateway_noise(LATE_LISTENER_USER_ID, 8)
        .await
        .unwrap();
    assert!(
        fake_voice
            .saw_late_dave_transition_ready_within(Duration::from_secs(2))
            .await,
        "live media driver must drain DAVE transition events during playback before more media"
    );
    let late_listener_decrypted = wait_for_late_listener_decryptable_audio(
        &fake_voice,
        SERVICE_USER_ID,
        fake_voice.audio_frame_count().await + 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(!late_listener_decrypted.is_empty());

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert_eq!(metrics.source_underrun_count, 0);
    assert_bounded_raw_egress_metrics(&metrics, "DAVE transition playback");
    assert!(metrics.playout_builder_prepare_duration.samples > 0);
    assert!(
        metrics.dave_transition_count_during_playback > 0,
        "metrics should count the injected DAVE transition during playback: {metrics:?}"
    );
    assert_eq!(
        metrics.stale_dave_send_prevented_count, 0,
        "late DAVE transition should finish before stale prepared media recovery is needed: {metrics:?}"
    );
    assert_eq!(
        metrics.dave_transition_recovery_reset_count, 0,
        "late DAVE transition should not reset the media clock during steady playback: {metrics:?}"
    );
    assert_eq!(
        metrics.controlled_media_interruption_count, 0,
        "late DAVE transition should not interrupt steady playback: {metrics:?}"
    );
    assert_eq!(
        metrics.recovery_media_boundary_count, 0,
        "late DAVE transition should not create a recovery media boundary: {metrics:?}"
    );
    assert_eq!(
        metrics.scheduled_silence_packet_count, 0,
        "late DAVE transition should not emit scheduled silence during steady playback: {metrics:?}"
    );
    assert_eq!(
        metrics.egress_inserted_silence_duration_ms, 0,
        "late DAVE transition should not create audible egress silence: {metrics:?}"
    );
    assert_eq!(
        metrics.track_tempo_window_slow_count, 0,
        "DAVE recovery silence must segment active music tempo windows: {metrics:?}"
    );
    assert!(
        metrics.all_packet_interval.max_ms <= 70,
        "late DAVE transition should keep RTP cadence bounded without scheduled silence: {metrics:?}"
    );
}

#[tokio::test]
async fn runtime_end_to_end_playback_stop_interrupts_in_flight_playback_without_track_ended() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(1).await >= 1);

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let follow_up_events = collect_events(&mut stream, 2).await;
    assert_eq!(follow_up_events.len(), 1);
    assert_eq!(follow_up_events[0].kind, SessionEventKind::Stopped as i32);
    assert!(
        follow_up_events
            .iter()
            .all(|event| event.kind != SessionEventKind::TrackEnded as i32)
    );

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.position_ms, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_stop_during_resolution_prevents_canceled_track_state_from_reappearing()
 {
    let fake_yt = FakeYtMusic::spawn().await;
    fake_yt.set_decipher_delay(Duration::from_millis(300)).await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let speaking_notified = speaking_observed.notified();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 3).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[2].current_video_id, "video-1");

    let stop_started = Instant::now();
    supervisor.send(Command::Stop).await.unwrap();
    let stop_elapsed = stop_started.elapsed();
    play_task.await.unwrap().unwrap();

    assert!(
        stop_elapsed < Duration::from_millis(150),
        "stop should not wait on slow playback preparation I/O: {stop_elapsed:?}"
    );

    let follow_up_events = collect_events(&mut stream, 4).await;
    assert_eq!(follow_up_events.len(), 1);
    assert_eq!(follow_up_events[0].kind, SessionEventKind::Stopped as i32);
    assert!(follow_up_events.iter().all(|event| {
        event.kind != SessionEventKind::TrackResolving as i32
            && event.kind != SessionEventKind::Buffering as i32
            && event.kind != SessionEventKind::Playing as i32
            && event.kind != SessionEventKind::TrackEnded as i32
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), speaking_notified)
            .await
            .is_err(),
        "pre-start stop should not emit a speaking update"
    );
    assert_eq!(fake_voice.audio_frame_count_at_least(0).await, 0);

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.position_ms, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_stop_then_replay_same_video_reaches_track_ended_again() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let interrupted_play = supervisor.clone();
    let play_task = tokio::spawn(async move {
        interrupted_play
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 5).await;
    assert_eq!(
        startup_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(startup_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[4].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(1).await >= 1);

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let stop_events = collect_events(&mut stream, 2).await;
    assert_eq!(stop_events.len(), 1);
    assert_eq!(stop_events[0].kind, SessionEventKind::Stopped as i32);

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let replay_events = collect_events(&mut stream, 4).await;
    assert_eq!(
        replay_events[0].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(replay_events[1].kind, SessionEventKind::Buffering as i32);
    assert_eq!(replay_events[2].kind, SessionEventKind::Playing as i32);
    assert_eq!(replay_events[3].kind, SessionEventKind::TrackEnded as i32);
}

#[tokio::test]
async fn runtime_end_to_end_playback_natural_end_then_replay_same_video_starts_from_beginning() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let initial_events = collect_events(&mut stream, 6).await;
    assert_eq!(
        initial_events[0].kind,
        SessionEventKind::VoiceConnecting as i32
    );
    assert_eq!(initial_events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        initial_events[2].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(initial_events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(initial_events[4].kind, SessionEventKind::Playing as i32);
    assert_eq!(initial_events[5].kind, SessionEventKind::TrackEnded as i32);

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let replay_events = collect_events(&mut stream, 4).await;
    assert_eq!(
        replay_events[0].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(replay_events[1].kind, SessionEventKind::Buffering as i32);
    assert_eq!(replay_events[2].kind, SessionEventKind::Playing as i32);
    assert_eq!(replay_events[3].kind, SessionEventKind::TrackEnded as i32);
}

async fn subscribe_events(
    supervisor: Supervisor,
) -> <ControlService as DiscordVoiceControl>::SubscribeEventsStream {
    ControlService {
        supervisor,
        readiness: Arc::new(Readiness::default()),
    }
    .subscribe_events(Request::new(SubscribeEventsRequest {}))
    .await
    .unwrap()
    .into_inner()
}

async fn collect_events<S>(stream: &mut S, count: usize) -> Vec<SessionEvent>
where
    S: futures::Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin,
{
    collect_events_with_timeout(stream, count, Duration::from_secs(2)).await
}

async fn collect_events_with_timeout<S>(
    stream: &mut S,
    count: usize,
    timeout: Duration,
) -> Vec<SessionEvent>
where
    S: futures::Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin,
{
    let deadline = Instant::now() + timeout;
    let mut events = Vec::with_capacity(count);
    while events.len() < count && Instant::now() < deadline {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(200), stream.next()).await
        {
            events.push(event.unwrap());
        }
    }
    events
}

async fn wait_for_late_listener_decryptable_audio(
    fake_voice: &FakeDiscordPeer,
    sender_user_id: &str,
    minimum_frame_count: usize,
    timeout: Duration,
) -> Vec<u8> {
    let deadline = Instant::now() + timeout;
    let mut next_frame_count = minimum_frame_count;
    let mut last_error = None;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(observed_count) = tokio::time::timeout(
            remaining,
            fake_voice.audio_frame_count_at_least(next_frame_count),
        )
        .await
        else {
            break;
        };

        match fake_voice
            .decrypt_last_dave_audio_frame_from_late_listener(sender_user_id)
            .await
        {
            Ok(payload) if !payload.is_empty() => return payload,
            Ok(_) => last_error = Some("empty DAVE payload".to_owned()),
            Err(err) => last_error = Some(err.to_string()),
        }
        next_frame_count = observed_count.saturating_add(1);
    }

    panic!(
        "late DAVE listener should decrypt post-transition media; last error: {}",
        last_error.unwrap_or_else(|| "no post-transition audio observed".to_owned())
    );
}

async fn audio_frame_times_at_least_with_timeout(
    fake_voice: &FakeDiscordPeer,
    minimum: usize,
    timeout: Duration,
) -> Vec<Instant> {
    let deadline = Instant::now() + timeout;
    let mut timestamps = Vec::new();
    while Instant::now() < deadline {
        timestamps = fake_voice.audio_frame_times_at_least(minimum).await;
        if timestamps.len() >= minimum {
            return timestamps;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    timestamps
}

fn find_five_packet_silence_tail(packets: &[ObservedAudioPacket], start: usize) -> usize {
    packets
        .windows(5)
        .enumerate()
        .skip(start)
        .find_map(|(index, window)| {
            window
                .iter()
                .all(|packet| packet.is_stop_silence)
                .then_some(index)
        })
        .expect("pause should emit five consecutive Opus silence RTP packets")
}

fn assert_five_packet_silence_tail(packets: &[ObservedAudioPacket], start: usize) {
    let tail = packets
        .get(start..start + 5)
        .expect("silence tail should contain five packets");
    assert!(
        tail.iter().all(|packet| packet.is_stop_silence),
        "pause tail packets must all be Opus silence frames: {tail:?}"
    );
    for pair in tail.windows(2) {
        assert_rtp_packet_follows(&pair[0], &pair[1], "pause silence tail packet");
    }
}

fn assert_rtp_packet_follows(
    previous: &ObservedAudioPacket,
    next: &ObservedAudioPacket,
    label: &str,
) {
    assert_eq!(
        next.sequence.wrapping_sub(previous.sequence),
        1,
        "{label} must advance the RTP sequence by one"
    );
    assert_eq!(
        next.timestamp.wrapping_sub(previous.timestamp),
        960,
        "{label} must advance the RTP timestamp by one 20ms Opus frame"
    );
}

fn intervals_between(timestamps: &[Instant]) -> Vec<Duration> {
    timestamps
        .windows(2)
        .map(|window| window[1].saturating_duration_since(window[0]))
        .collect()
}

#[derive(Debug)]
struct IntervalStats {
    max: Duration,
    p05: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

fn interval_stats(intervals: &[Duration]) -> IntervalStats {
    assert!(!intervals.is_empty(), "interval stats require samples");
    let mut sorted = intervals.to_vec();
    sorted.sort_unstable();
    IntervalStats {
        max: sorted[sorted.len() - 1],
        p05: percentile_duration(&sorted, 5),
        p50: percentile_duration(&sorted, 50),
        p95: percentile_duration(&sorted, 95),
        p99: percentile_duration(&sorted, 99),
    }
}

fn assert_track_tempo_metrics_within_bounds(metrics: &PlaybackStabilitySnapshot, label: &str) {
    assert!(
        metrics.track_media_to_wall_clock_ratio_ppm >= MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM,
        "{label} metrics must reject slower-than-real-time playback: {metrics:?}"
    );
    assert!(
        metrics.track_media_to_wall_clock_ratio_ppm <= MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM,
        "{label} metrics must reject faster-than-real-time playback: {metrics:?}"
    );
    assert!(
        metrics.track_tempo_window_count > 0,
        "{label} metrics should report rolling tempo windows: {metrics:?}"
    );
    assert!(
        metrics.track_tempo_window_min_ratio_ppm >= MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM,
        "{label} rolling tempo windows must not be slower than real time: {metrics:?}"
    );
    assert!(
        metrics.track_tempo_window_max_ratio_ppm <= MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM,
        "{label} rolling tempo windows must not be faster than real time: {metrics:?}"
    );
    assert_eq!(
        metrics.track_tempo_window_fast_count, 0,
        "{label} metrics should not report faster-than-real-time rolling windows: {metrics:?}"
    );
    assert_eq!(
        metrics.track_tempo_window_slow_count, 0,
        "{label} metrics should not report slower-than-real-time rolling windows: {metrics:?}"
    );
    assert_eq!(
        metrics.track_fast_interval_count, 0,
        "{label} metrics must not hide shortened local send-start intervals: {metrics:?}"
    );
    assert_eq!(
        metrics.track_fast_interval_min_us, 0,
        "{label} metrics must not report a shortened local send-start interval: {metrics:?}"
    );
}

fn assert_bounded_raw_egress_metrics(metrics: &PlaybackStabilitySnapshot, label: &str) {
    assert_raw_egress_metrics_published(metrics, label);
    assert!(
        metrics.max_egress_buffer_depth.duration_ms > 0,
        "{label} should exercise the prepared Discord playout buffer: {metrics:?}"
    );
    assert!(
        metrics
            .active_pre_pause_prepared_track_queue_depth
            .sample_count
            > 0,
        "{label} should expose active pre-pause prepared track queue samples: {metrics:?}"
    );
    assert_eq!(
        metrics
            .active_pre_pause_prepared_track_queue_depth
            .empty_count,
        0,
        "{label} should not report empty active pre-pause prepared track queue samples: {metrics:?}"
    );
    assert!(
        (340..=460).contains(
            &metrics
                .active_pre_pause_prepared_track_queue_depth
                .p50_depth
                .duration_ms
        ),
        "{label} should keep the active pre-pause prepared track queue centered near 400ms: {metrics:?}"
    );
}

fn assert_raw_egress_metrics_published(metrics: &PlaybackStabilitySnapshot, label: &str) {
    assert_eq!(
        metrics.egress_buffer_target_ms, DISCORD_EGRESS_BUFFER_TARGET_MS,
        "{label} should publish the Discord egress reservoir target: {metrics:?}"
    );
    assert!(
        metrics.max_egress_buffer_depth.duration_ms <= DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS,
        "{label} egress buffer must remain bounded by the high watermark: {metrics:?}"
    );
    assert!(
        metrics.prepared_rtp_queue_depth_ms <= DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS,
        "{label} prepared RTP queue must remain bounded by the high watermark: {metrics:?}"
    );
    assert_eq!(
        metrics.prepared_track_queue_target_ms, DISCORD_EGRESS_BUFFER_TARGET_MS,
        "{label} should publish the prepared track queue target: {metrics:?}"
    );
    assert!(
        metrics.prepared_track_queue_low_watermark_ms >= 300,
        "{label} should publish a strict prepared track queue low watermark: {metrics:?}"
    );
    assert!(
        metrics.prepared_track_queue_high_watermark_ms <= DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS,
        "{label} prepared track queue high watermark must not exceed the hard cap: {metrics:?}"
    );
}

fn assert_fake_udp_observed_intervals_not_bursty(stats: &IntervalStats, label: &str) {
    assert!(
        stats.p05 >= MIN_FAKE_UDP_OBSERVED_INTERVAL,
        "{label} fake UDP p05 should reject burst catch-up while allowing receive timestamp jitter: {stats:?}"
    );
}

fn assert_no_fake_udp_interval_bursty(intervals: &[Duration], label: &str) {
    let stats = interval_stats(intervals);
    assert_fake_udp_observed_intervals_not_bursty(&stats, label);
}

fn percentile_duration(sorted: &[Duration], percentile: usize) -> Duration {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}

struct CpuContentionGuard {
    stop: Arc<AtomicBool>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl CpuContentionGuard {
    fn start(worker_count: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..worker_count)
            .map(|_| {
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let busy_until = StdInstant::now() + Duration::from_millis(2);
                        while StdInstant::now() < busy_until && !stop.load(Ordering::Relaxed) {
                            hint::spin_loop();
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        Self { stop, workers }
    }
}

impl Drop for CpuContentionGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}
