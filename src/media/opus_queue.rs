use std::collections::VecDeque;

use bytes::Bytes;

use crate::media::position::SharedPlaybackPosition;

#[derive(Clone, Debug)]
pub struct OpusFrame {
    pub data: Bytes,
    pub duration_ms: u64,
    tracker: Option<SharedPlaybackPosition>,
}

impl OpusFrame {
    pub fn new(data: Bytes, duration_ms: u64) -> Self {
        Self {
            data,
            duration_ms,
            tracker: None,
        }
    }

    pub(crate) fn tracked(data: Bytes, duration_ms: u64, tracker: SharedPlaybackPosition) -> Self {
        Self {
            data,
            duration_ms,
            tracker: Some(tracker),
        }
    }
}

impl PartialEq for OpusFrame {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data && self.duration_ms == other.duration_ms
    }
}

impl Eq for OpusFrame {}

#[derive(Debug)]
pub struct OpusFrameQueue {
    capacity: usize,
    inner: VecDeque<OpusFrame>,
}

impl OpusFrameQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, frame: OpusFrame) -> Result<(), OpusFrame> {
        if self.inner.len() >= self.capacity {
            return Err(frame);
        }

        self.inner.push_back(frame);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<OpusFrame> {
        let frame = self.inner.pop_front()?;
        if let Some(tracker) = &frame.tracker {
            tracker
                .lock()
                .unwrap()
                .record_sent_packet(frame.duration_ms);
        }
        Some(frame)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
