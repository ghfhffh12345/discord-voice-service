use bytes::Bytes;
use discord_voice_service::media::opus_queue::{OpusFrame, OpusFrameQueue};

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
