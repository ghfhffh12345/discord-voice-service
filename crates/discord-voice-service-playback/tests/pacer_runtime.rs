use discord_voice_service_playback::pacer::{AudioPacer, PacedPacketKind};
use tokio::task::yield_now;
use tokio::time::{Duration, Instant, advance};

#[tokio::test(start_paused = true)]
async fn pacer_runtime_emits_one_audio_frame_per_20ms_tick() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    pacer.tick().await;
    assert_eq!(start.elapsed(), Duration::ZERO);

    let next_tick = tokio::spawn(async move {
        pacer.tick().await;
        pacer
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(19)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let pacer = next_tick.await.unwrap();

    assert_eq!(start.elapsed(), Duration::from_millis(20));
    assert_eq!(pacer.emitted_frames(), 2);
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_waits_for_each_frame_duration() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    pacer.wait_for(Duration::from_millis(60)).await;
    assert_eq!(start.elapsed(), Duration::ZERO);

    let next_tick = tokio::spawn(async move {
        pacer.wait_for(Duration::from_millis(10)).await;
        pacer
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(59)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let mut pacer = next_tick.await.unwrap();

    assert_eq!(start.elapsed(), Duration::from_millis(60));
    assert_eq!(pacer.emitted_frames(), 2);

    let next_tick = tokio::spawn(async move {
        pacer.wait_for(Duration::from_millis(20)).await;
        pacer
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(9)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let pacer = next_tick.await.unwrap();

    assert_eq!(start.elapsed(), Duration::from_millis(70));
    assert_eq!(pacer.emitted_frames(), 3);
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_keeps_scheduled_cadence_for_jitter_inside_tolerance() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    pacer.wait_until_ready().await;
    advance(Duration::from_millis(2)).await;
    pacer.mark_emitted(Duration::from_millis(20));

    let next_tick = tokio::spawn(async move {
        pacer.wait_for(Duration::from_millis(20)).await;
        pacer
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(15)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let pacer = next_tick.await.unwrap();

    assert_eq!(start.elapsed(), Duration::from_millis(20));
    assert_eq!(pacer.emitted_frames(), 2);
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_does_not_burst_after_slow_send() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    pacer.wait_until_ready().await;
    advance(Duration::from_millis(100)).await;
    pacer.mark_emitted(Duration::from_millis(20));

    let next_tick = tokio::spawn(async move {
        pacer.wait_for(Duration::from_millis(20)).await;
        pacer
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(7)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let pacer = next_tick.await.unwrap();

    assert_eq!(start.elapsed(), Duration::from_millis(120));
    assert_eq!(pacer.emitted_frames(), 2);
    assert_eq!(pacer.clock_reset_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_whole_frame_lateness_records_explicit_recovery() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    advance(Duration::from_millis(45)).await;
    let mark = pacer.mark_sent(
        PacedPacketKind::Track,
        start,
        Duration::from_millis(20),
        Instant::now(),
    );

    assert!(mark.media_clock_reset);
    assert!(mark.tempo_rebased);
    assert_eq!(pacer.clock_reset_count(), 1);
    assert_eq!(pacer.tempo_rebase_count(), 1);
    assert_eq!(
        pacer.next_deadline().saturating_duration_since(start),
        Duration::from_millis(65)
    );
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_rebases_track_cadence_after_material_sub_frame_late_send() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    advance(Duration::from_millis(12)).await;
    let mark = pacer.mark_sent(
        PacedPacketKind::Track,
        start,
        Duration::from_millis(20),
        Instant::now(),
    );
    assert!(!mark.media_clock_reset);
    assert!(mark.tempo_rebased);

    let next_tick = tokio::spawn(async move {
        pacer.wait_until_ready().await;
        start.elapsed()
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(19)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let next_sent_at = next_tick.await.unwrap();

    assert_eq!(next_sent_at, Duration::from_millis(32));
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_very_late_sub_frame_send_waits_another_full_track_frame() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    advance(Duration::from_millis(18)).await;
    let mark = pacer.mark_sent(
        PacedPacketKind::Track,
        start,
        Duration::from_millis(20),
        Instant::now(),
    );
    assert!(!mark.media_clock_reset);
    assert!(mark.tempo_rebased);

    let next_tick = tokio::spawn(async move {
        pacer.wait_until_ready().await;
        start.elapsed()
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(19)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let next_sent_at = next_tick.await.unwrap();

    assert_eq!(next_sent_at, Duration::from_millis(38));
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_repeated_eighteen_ms_late_sends_do_not_fast_follow() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();
    let mut sent_at = Vec::new();

    for _ in 0..3 {
        let scheduled = pacer.next_deadline();
        pacer.wait_until_ready().await;
        advance(Duration::from_millis(18)).await;
        let sent = Instant::now();
        sent_at.push(sent.saturating_duration_since(start));
        let mark = pacer.mark_sent(
            PacedPacketKind::Track,
            scheduled,
            Duration::from_millis(20),
            sent,
        );
        assert!(!mark.media_clock_reset);
        assert!(mark.tempo_rebased);
        assert_eq!(pacer.next_deadline(), sent + Duration::from_millis(20));
    }

    let intervals = sent_at
        .windows(2)
        .map(|window| window[1] - window[0])
        .collect::<Vec<_>>();
    assert_eq!(
        intervals,
        vec![Duration::from_millis(38), Duration::from_millis(38)]
    );
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_repeated_post_send_overhead_does_not_change_twenty_ms_cadence() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();
    let mut scheduled_deadlines = Vec::new();
    let mut sent_at = Vec::new();

    for overhead_ms in [3, 5, 7, 10] {
        let scheduled = pacer.next_deadline();
        scheduled_deadlines.push(scheduled.saturating_duration_since(start));
        pacer.wait_until_ready().await;
        sent_at.push(scheduled.saturating_duration_since(start));
        advance(Duration::from_millis(overhead_ms)).await;
        let mark = pacer.mark_sent(
            PacedPacketKind::Track,
            scheduled,
            Duration::from_millis(20),
            scheduled,
        );
        assert!(!mark.media_clock_reset);
        assert!(!mark.tempo_rebased);
    }

    assert_eq!(
        scheduled_deadlines,
        vec![
            Duration::ZERO,
            Duration::from_millis(20),
            Duration::from_millis(40),
            Duration::from_millis(60),
        ]
    );
    assert_eq!(
        pacer.next_deadline().saturating_duration_since(start),
        Duration::from_millis(80)
    );
    let intervals = sent_at
        .windows(2)
        .map(|window| window[1] - window[0])
        .collect::<Vec<_>>();
    assert_eq!(
        intervals,
        vec![
            Duration::from_millis(20),
            Duration::from_millis(20),
            Duration::from_millis(20)
        ]
    );
    assert_eq!(pacer.tempo_rebase_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_gap_between_frames_does_not_trigger_catchup_burst() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();
    let mut sent_at = Vec::new();

    pacer.wait_until_ready().await;
    sent_at.push(start.elapsed());
    pacer.mark_emitted(Duration::from_millis(20));

    advance(Duration::from_millis(85)).await;
    pacer.wait_until_ready().await;
    sent_at.push(start.elapsed());
    pacer.mark_emitted(Duration::from_millis(20));

    let next_tick = tokio::spawn(async move {
        pacer.wait_for(Duration::from_millis(20)).await;
        (pacer, start.elapsed())
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(19)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let (pacer, third_sent_at) = next_tick.await.unwrap();
    sent_at.push(third_sent_at);

    let intervals = sent_at
        .windows(2)
        .map(|window| window[1] - window[0])
        .collect::<Vec<_>>();
    assert_eq!(
        intervals,
        vec![Duration::from_millis(85), Duration::from_millis(20)]
    );
    assert_eq!(pacer.emitted_frames(), 3);
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_reset_after_pause_resumes_without_catchup_burst() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    pacer.wait_until_ready().await;
    let first_sent_at = Instant::now();
    let scheduled = pacer.next_deadline();
    let mark = pacer.mark_sent(
        PacedPacketKind::Track,
        scheduled,
        Duration::from_millis(20),
        first_sent_at,
    );
    assert!(!mark.media_clock_reset);
    assert!(!mark.tempo_rebased);

    advance(Duration::from_millis(500)).await;
    let resumed_at = Instant::now();
    pacer.reset_after_interruption_at(resumed_at, Duration::ZERO);
    assert_eq!(pacer.clock_reset_count(), 1);

    pacer.wait_until_ready().await;
    let scheduled = pacer.next_deadline();
    let mark = pacer.mark_sent(
        PacedPacketKind::Track,
        scheduled,
        Duration::from_millis(20),
        Instant::now(),
    );
    assert!(!mark.media_clock_reset);
    assert!(!mark.tempo_rebased);

    let next_tick = tokio::spawn(async move {
        pacer.wait_until_ready().await;
        start.elapsed()
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(19)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let next_sent_at = next_tick.await.unwrap();

    assert_eq!(next_sent_at, Duration::from_millis(520));
}
