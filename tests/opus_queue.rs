use bytes::Bytes;
use discord_voice_service::media::opus_queue::OpusFrameQueue;

#[test]
fn queue_enforces_capacity_and_fifo_order() {
    let mut queue = OpusFrameQueue::new(2);
    assert!(queue.push(Bytes::from_static(b"a")).is_ok());
    assert!(queue.push(Bytes::from_static(b"b")).is_ok());
    assert!(queue.push(Bytes::from_static(b"c")).is_err());
    assert_eq!(queue.pop().unwrap(), Bytes::from_static(b"a"));
    assert_eq!(queue.pop().unwrap(), Bytes::from_static(b"b"));
}
