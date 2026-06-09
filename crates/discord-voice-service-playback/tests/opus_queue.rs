use bytes::Bytes;
use discord_voice_service_playback::media::opus_queue::{
    OpusFrame, OpusFrameQueue, duration_from_samples, opus_packet_duration,
};
use std::time::Duration;

#[test]
fn opus_packet_duration_reads_samples_from_packet_toc() {
    let silence_20ms = [0xF8, 0xFF, 0xFE];
    let duration = opus_packet_duration(&silence_20ms).expect("silence frame duration");

    assert_eq!(duration.ms, 20);
    assert_eq!(duration.samples, 960);
    assert!(opus_packet_duration(&[]).is_none());
    assert!(opus_packet_duration(&[0x03]).is_none());
}

#[test]
fn opus_packet_duration_preserves_fractional_millisecond_samples() {
    let celt_2_5ms = [0x80];
    let duration = opus_packet_duration(&celt_2_5ms).expect("2.5ms frame duration");

    assert_eq!(duration.samples, 120);
    assert_eq!(duration.ms, 2);
    assert_eq!(
        duration_from_samples(u64::from(duration.samples)),
        Duration::from_micros(2_500)
    );
}

#[test]
fn queue_enforces_capacity_and_fifo_order() {
    let mut queue = OpusFrameQueue::new(2);
    assert!(
        queue
            .push(OpusFrame::new(Bytes::from_static(b"a"), 20))
            .is_ok()
    );
    assert!(
        queue
            .push(OpusFrame::new(Bytes::from_static(b"b"), 40))
            .is_ok()
    );
    assert!(
        queue
            .push(OpusFrame::new(Bytes::from_static(b"c"), 60))
            .is_err()
    );
    assert_eq!(
        queue.pop().unwrap(),
        OpusFrame::new(Bytes::from_static(b"a"), 20)
    );
    assert_eq!(
        queue.pop().unwrap(),
        OpusFrame::new(Bytes::from_static(b"b"), 40)
    );
}

#[test]
fn queue_depth_uses_samples_instead_of_summing_floored_packet_ms() {
    let mut queue = OpusFrameQueue::with_resource_limits(4, 16, 5);
    queue
        .push(OpusFrame::with_duration_samples(
            Bytes::from_static(b"a"),
            2,
            120,
        ))
        .unwrap();
    queue
        .push(OpusFrame::with_duration_samples(
            Bytes::from_static(b"b"),
            2,
            120,
        ))
        .unwrap();

    let depth = queue.depth();
    assert_eq!(depth.packets, 2);
    assert_eq!(depth.duration_samples, 240);
    assert_eq!(depth.duration_ms, 5);
    assert!(queue.is_full());
}

#[test]
fn queue_tracks_duration_samples_and_bytes() {
    let mut queue = OpusFrameQueue::with_resource_limits(4, 4, 40);
    queue
        .push(OpusFrame::with_duration_samples(
            Bytes::from_static(b"aa"),
            10,
            480,
        ))
        .unwrap();
    queue
        .push(OpusFrame::with_duration_samples(
            Bytes::from_static(b"bb"),
            20,
            960,
        ))
        .unwrap();

    let depth = queue.depth();
    assert_eq!(depth.packets, 2);
    assert_eq!(depth.bytes, 4);
    assert_eq!(depth.duration_ms, 30);
    assert_eq!(depth.duration_samples, 1_440);
    assert!(queue.is_full());
    assert!(
        queue
            .push(OpusFrame::new(Bytes::from_static(b"c"), 20))
            .is_err(),
        "byte cap should reject frames even when packet capacity remains"
    );

    let first = queue.pop().unwrap();
    assert_eq!(first.data, Bytes::from_static(b"aa"));
    let depth = queue.depth();
    assert_eq!(depth.packets, 1);
    assert_eq!(depth.bytes, 2);
    assert_eq!(depth.duration_ms, 20);
    assert_eq!(depth.duration_samples, 960);

    assert!(
        queue
            .push(OpusFrame::new(Bytes::from_static(b"cc"), 25))
            .is_err(),
        "duration cap should reject frames even when byte capacity remains"
    );
}

#[test]
fn queue_can_restore_popped_frame_to_front() {
    let mut queue = OpusFrameQueue::new(3);
    let first = OpusFrame::new(Bytes::from_static(b"a"), 20);
    let second = OpusFrame::new(Bytes::from_static(b"b"), 40);
    queue.push(first.clone()).unwrap();
    queue.push(second.clone()).unwrap();

    let popped = queue.pop().unwrap();
    assert_eq!(popped, first);
    queue.push_front(popped).unwrap();

    let depth = queue.depth();
    assert_eq!(depth.packets, 2);
    assert_eq!(depth.duration_ms, 60);
    assert_eq!(depth.bytes, 2);
    assert_eq!(queue.pop().unwrap(), first);
    assert_eq!(queue.pop().unwrap(), second);
}

#[test]
fn queue_can_represent_five_seconds_of_opus_frames() {
    const TARGET_BUFFER_MS: u64 = 5_000;
    const FRAME_DURATION_MS: u64 = 20;
    const FRAME_SAMPLES: u32 = 960;
    const FRAME_BYTES: usize = 64;
    const FRAME_COUNT: usize = (TARGET_BUFFER_MS / FRAME_DURATION_MS) as usize;

    let mut queue =
        OpusFrameQueue::with_resource_limits(FRAME_COUNT, 4 * 1024 * 1024, TARGET_BUFFER_MS);

    for _ in 0..FRAME_COUNT {
        queue
            .push(OpusFrame::with_duration_samples(
                Bytes::from(vec![0_u8; FRAME_BYTES]),
                FRAME_DURATION_MS,
                FRAME_SAMPLES,
            ))
            .expect("five-second queue should accept a 20ms Opus frame");
    }

    let depth = queue.depth();
    assert_eq!(depth.packets, FRAME_COUNT);
    assert_eq!(depth.bytes, FRAME_COUNT * FRAME_BYTES);
    assert_eq!(depth.duration_ms, TARGET_BUFFER_MS);
    assert_eq!(
        depth.duration_samples,
        u64::from(FRAME_SAMPLES) * FRAME_COUNT as u64
    );
    assert!(queue.is_full());
    assert!(
        queue
            .push(OpusFrame::with_duration_samples(
                Bytes::from(vec![1_u8; FRAME_BYTES]),
                FRAME_DURATION_MS,
                FRAME_SAMPLES,
            ))
            .is_err(),
        "duration cap should bound the producer/source buffer at exactly five seconds"
    );
}
