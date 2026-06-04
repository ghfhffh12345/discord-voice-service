use discord_voice_service_playback::pacer::AudioPacer;
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

    advance(Duration::from_millis(7)).await;
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
async fn pacer_runtime_does_not_add_send_overhead_to_frame_cadence() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    pacer.wait_until_ready().await;
    advance(Duration::from_millis(4)).await;
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
async fn pacer_runtime_keeps_scheduled_cadence_after_sub_frame_late_send() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    advance(Duration::from_millis(12)).await;
    assert!(!pacer.mark_sent(start, Duration::from_millis(20), Instant::now()));

    let next_tick = tokio::spawn(async move {
        pacer.wait_until_ready().await;
        start.elapsed()
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(8)).await;
    yield_now().await;
    let next_sent_at = next_tick.await.unwrap();

    assert_eq!(next_sent_at, Duration::from_millis(20));
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_applies_only_sub_frame_spacing_clamp_after_very_late_sub_frame_send() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();

    advance(Duration::from_millis(18)).await;
    assert!(!pacer.mark_sent(start, Duration::from_millis(20), Instant::now()));

    let next_tick = tokio::spawn(async move {
        pacer.wait_until_ready().await;
        start.elapsed()
    });
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(7)).await;
    yield_now().await;
    assert!(!next_tick.is_finished());

    advance(Duration::from_millis(1)).await;
    yield_now().await;
    let next_sent_at = next_tick.await.unwrap();

    assert_eq!(next_sent_at, Duration::from_millis(26));
}

#[tokio::test(start_paused = true)]
async fn pacer_runtime_repeated_send_overhead_does_not_change_twenty_ms_cadence() {
    let mut pacer = AudioPacer::new();
    let start = Instant::now();
    let mut scheduled_deadlines = Vec::new();

    for overhead_ms in [3, 5, 7, 10] {
        let scheduled = pacer.next_deadline();
        scheduled_deadlines.push(scheduled.saturating_duration_since(start));
        pacer.wait_until_ready().await;
        advance(Duration::from_millis(overhead_ms)).await;
        assert!(!pacer.mark_sent(scheduled, Duration::from_millis(20), Instant::now()));
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
