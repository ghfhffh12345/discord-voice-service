use std::collections::VecDeque;

use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct OpusFrame {
    pub data: Bytes,
    pub duration_ms: u64,
}

impl OpusFrame {
    pub fn new(data: Bytes, duration_ms: u64) -> Self {
        Self { data, duration_ms }
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
        self.inner.pop_front()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
