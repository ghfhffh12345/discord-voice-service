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
use discord_voice_service_runtime::{Command, ControlService, Readiness, SessionState, Supervisor};
use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
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
    assert!(
        metrics.max_playout_buffer_depth.duration_ms == 0,
        "Playing must not fill a prepared RTP playout buffer: {metrics:?}"
    );
    assert_eq!(metrics.current_playout_buffer_depth.duration_ms, 0);
    assert_eq!(metrics.prepared_rtp_queue_depth_ms, 0);
    assert_eq!(metrics.playout_builder_prepare_duration.samples, 0);
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
        stats.min >= Duration::from_millis(10),
        "normal playback should not burst packets together: {stats:?}"
    );
    assert!(
        stats.p95 <= Duration::from_millis(45),
        "normal playback p95 should stay near 20ms: {stats:?}"
    );
    assert!(
        stats.p99 <= Duration::from_millis(50),
        "normal playback p99 should stay bounded without refill stalls: {stats:?}"
    );
    assert!(
        stats.max < Duration::from_millis(100),
        "normal playback must not have a perceptible >=100ms interval: {stats:?}"
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
        metrics.track_interval.min_ms >= 10,
        "runtime metrics should detect packet bursts: {metrics:?}"
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
    assert!(
        metrics.max_playout_buffer_depth.duration_ms == 0,
        "normal playback must not fill a prepared RTP playout buffer: {metrics:?}"
    );
    assert_eq!(metrics.current_playout_buffer_depth.duration_ms, 0);
    assert_eq!(metrics.prepared_rtp_queue_depth_ms, 0);
    assert!(metrics.source_producer_fill_duration.samples >= 1);
    assert_eq!(metrics.playout_builder_prepare_duration.samples, 0);
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
        Duration::from_millis(8),
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
    assert!(
        metrics.track_interval.min_ms >= 15,
        "stress metrics must not show catch-up bursts: {metrics:?}"
    );
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
    assert!(
        metrics.max_playout_buffer_depth.duration_ms == 0,
        "stress playback must not fill a prepared RTP playout buffer: {metrics:?}"
    );
    assert_eq!(metrics.current_playout_buffer_depth.duration_ms, 0);
    assert_eq!(metrics.prepared_rtp_queue_depth_ms, 0);
    assert!(metrics.source_producer_fill_duration.samples >= 1);
    assert_eq!(metrics.playout_builder_prepare_duration.samples, 0);
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
async fn runtime_end_to_end_playback_repeated_send_delay_keeps_twenty_ms_output_cadence() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-long.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    supervisor.set_live_media_send_delay_for_tests(|packet_index| {
        if packet_index < 70 {
            Some(Duration::from_millis(5))
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
    eprintln!("repeated delayed-send cadence: {stats:?}");
    assert!(
        stats.p50 <= Duration::from_millis(23),
        "repeated 5ms send-path delay must not stretch cadence toward 25ms: {stats:?}"
    );
    assert!(
        stats.p95 <= Duration::from_millis(35),
        "repeated 5ms send-path delay should keep p95 near cadence: {stats:?}"
    );
    assert!(
        stats.max < Duration::from_millis(100),
        "repeated 5ms send-path delay must not create perceptible gaps: {stats:?}"
    );

    supervisor.send(Command::Stop).await.unwrap();
    supervisor.clear_live_media_send_delay_for_tests();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert!(
        metrics.track_interval.p50_ms <= 23,
        "metrics must show scheduled-clock cadence despite repeated send delay: {metrics:?}"
    );
    assert_eq!(metrics.scheduler_late_reset_count, 0);
    assert_eq!(metrics.controlled_media_interruption_count, 0);
    assert_eq!(metrics.source_underrun_count, 0);
}

#[tokio::test]
async fn runtime_end_to_end_playback_media_driver_does_not_burst_after_pause_or_late_tick() {
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
        "test hook should have delayed one live media send"
    );
    let intervals = intervals_between(&timestamps[..45]);
    let stats = interval_stats(&intervals);
    let delayed_interval = intervals[19];
    let post_delay_interval = intervals[20];
    assert!(
        delayed_interval >= Duration::from_millis(250),
        "the hook must create a genuinely late media tick: {delayed_interval:?}; stats={stats:?}"
    );
    assert!(
        post_delay_interval >= Duration::from_millis(10),
        "late media tick recovery must not catch up with a burst: {post_delay_interval:?}; stats={stats:?}"
    );
    assert!(
        post_delay_interval <= Duration::from_millis(70),
        "late media tick recovery should resume near cadence: {post_delay_interval:?}; stats={stats:?}"
    );
    assert!(
        stats.min >= Duration::from_millis(10),
        "late media tick recovery must not burst packets together: {stats:?}"
    );

    supervisor.send(Command::Stop).await.unwrap();
    supervisor.clear_live_media_send_delay_for_tests();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert!(
        metrics.playout_sender_lateness.max_ms >= 250,
        "metrics should observe the injected late media tick: {metrics:?}"
    );
    assert!(metrics.track_interval.min_ms >= 10);
    assert_eq!(metrics.playout_underrun_count, 0);
    assert_eq!(metrics.source_underrun_count, 0);
    assert_eq!(metrics.sender_forbidden_work_count, 0);
    assert_eq!(metrics.current_playout_buffer_depth.duration_ms, 0);
    assert_eq!(metrics.max_playout_buffer_depth.duration_ms, 0);
    assert_eq!(metrics.prepared_rtp_queue_depth_ms, 0);
    assert_eq!(metrics.playout_builder_prepare_duration.samples, 0);
    assert!(
        metrics.scheduler_late_reset_count >= 1,
        "late tick should be reported as an explicit scheduler reset: {metrics:?}"
    );
    assert!(
        metrics.media_clock_reset_count >= 1,
        "late tick should rebase the media clock explicitly: {metrics:?}"
    );
    assert!(
        metrics.controlled_media_interruption_count >= 1,
        "late tick should be an explicit interruption, not hidden normal playback: {metrics:?}"
    );
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

    let speaking_zeros_before_pause = fake_voice.speaking_state_count(0).await;
    supervisor.send(Command::Pause).await.unwrap();
    let pause_events = collect_events(&mut stream, 1).await;
    assert_eq!(pause_events[0].kind, SessionEventKind::Paused as i32);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        fake_voice.speaking_state_count(0).await,
        speaking_zeros_before_pause,
        "pause must not stop Speaking for transient playback suspension"
    );

    supervisor.send(Command::Pause).await.unwrap();
    tokio::time::timeout(Duration::from_millis(80), stream.next())
        .await
        .expect_err("redundant Pause while already paused must not emit a playback event");
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "redundant Pause while already paused must not reconnect voice media"
    );

    tokio::time::sleep(Duration::from_millis(80)).await;
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
    supervisor.send(Command::Resume).await.unwrap();
    let resume_events = collect_events(&mut stream, 1).await;
    assert_eq!(resume_events[0].kind, SessionEventKind::Playing as i32);
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "resume should not rebuild voice media for a transient pause"
    );
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert_eq!(
        fake_voice.speaking_state_count(1).await,
        speaking_ones_before_resume,
        "resume must not restart Speaking when the transport was not rebuilt"
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

    let speaking_zeros_before_pause = fake_voice.speaking_state_count(0).await;
    supervisor.send(Command::Pause).await.unwrap();
    let pause_events = collect_events(&mut stream, 1).await;
    assert_eq!(pause_events[0].kind, SessionEventKind::Paused as i32);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        fake_voice.speaking_state_count(0).await,
        speaking_zeros_before_pause,
        "pause must keep Speaking active for transient suspension"
    );

    let paused_count = fake_voice.audio_frame_count().await;
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
    let playing_observed_at = Instant::now();
    assert_eq!(resume_events[0].kind, SessionEventKind::Playing as i32);
    assert_eq!(
        fake_voice.discovery_count().await,
        1,
        "resume without a paused voice context refresh must not reconnect voice media"
    );
    assert_eq!(
        fake_voice.speaking_state_count(1).await,
        speaking_ones_before_resume,
        "resume without transport rebuild must not prepare Speaking again"
    );

    let resumed_target = paused_count + 4;
    assert!(fake_voice.audio_frame_count_at_least(resumed_target).await >= resumed_target);
    let resumed_non_silence_target = non_silence_frames_before_resume + 5;
    let resumed_non_silence_times = fake_voice
        .non_silence_audio_frame_times_at_least(resumed_non_silence_target)
        .await;
    let first_resumed_interval = resumed_non_silence_times[non_silence_frames_before_resume]
        .checked_duration_since(playing_observed_at)
        .unwrap_or(Duration::ZERO);
    assert!(
        first_resumed_interval <= Duration::from_millis(40),
        "first resumed non-silence frame should arrive promptly after Playing: {first_resumed_interval:?}"
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
    assert!(
        resumed_stats.min >= Duration::from_millis(10),
        "resume should not burst non-silence packets together: {resumed_stats:?}"
    );
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
            .all(|interval_ms| *interval_ms >= 10),
        "resume metrics should not show burst intervals: {metrics:?}"
    );
    assert_eq!(metrics.buffer_underrun_count, 0);
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
    let frames_after_transition_ready = fake_voice.audio_frame_count().await;
    assert!(
        fake_voice
            .audio_frame_count_at_least(frames_after_transition_ready + 2)
            .await
            >= frames_after_transition_ready + 2
    );
    let late_listener_decrypted = fake_voice
        .decrypt_last_dave_audio_frame_from_late_listener(SERVICE_USER_ID)
        .await
        .expect("late DAVE listener should decrypt post-transition media");
    assert!(!late_listener_decrypted.is_empty());

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let metrics = supervisor
        .playback_metrics()
        .await
        .expect("playback should publish stability metrics");
    assert_eq!(metrics.source_underrun_count, 0);
    assert_eq!(metrics.current_playout_buffer_depth.duration_ms, 0);
    assert_eq!(metrics.max_playout_buffer_depth.duration_ms, 0);
    assert_eq!(metrics.prepared_rtp_queue_depth_ms, 0);
    assert_eq!(metrics.playout_builder_prepare_duration.samples, 0);
    assert!(
        metrics.dave_transition_count_during_playback > 0,
        "metrics should count the injected DAVE transition during playback: {metrics:?}"
    );
    if metrics.stale_dave_send_prevented_count > 0 {
        assert!(
            metrics.dave_transition_recovery_reset_count > 0,
            "stale DAVE prevention must be paired with DAVE recovery reset evidence: {metrics:?}"
        );
        assert!(
            metrics.controlled_media_interruption_count > 0,
            "stale DAVE prevention must be reported as a controlled interruption: {metrics:?}"
        );
    } else {
        assert_eq!(metrics.dave_transition_recovery_reset_count, 0);
        assert_eq!(metrics.controlled_media_interruption_count, 0);
    }
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

fn intervals_between(timestamps: &[Instant]) -> Vec<Duration> {
    timestamps
        .windows(2)
        .map(|window| window[1].saturating_duration_since(window[0]))
        .collect()
}

#[derive(Debug)]
struct IntervalStats {
    min: Duration,
    max: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

fn interval_stats(intervals: &[Duration]) -> IntervalStats {
    assert!(!intervals.is_empty(), "interval stats require samples");
    let mut sorted = intervals.to_vec();
    sorted.sort_unstable();
    IntervalStats {
        min: sorted[0],
        max: sorted[sorted.len() - 1],
        p50: percentile_duration(&sorted, 50),
        p95: percentile_duration(&sorted, 95),
        p99: percentile_duration(&sorted, 99),
    }
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
