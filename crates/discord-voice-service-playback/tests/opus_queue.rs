use bytes::Bytes;
use discord_voice_service_playback::media::opus_queue::{OpusFrame, OpusFrameQueue};

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
