use discord_voice_service::playback::pacer::AudioPacer;
use tokio::task::yield_now;
use tokio::time::{Duration, Instant, advance};

#[tokio::test(start_paused = true)]
async fn runtime_emits_one_audio_frame_per_20ms_tick() {
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
