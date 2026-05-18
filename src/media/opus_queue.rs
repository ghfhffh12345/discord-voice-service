use std::collections::VecDeque;

use bytes::Bytes;

#[derive(Debug)]
pub struct OpusFrameQueue {
    capacity: usize,
    inner: VecDeque<Bytes>,
}

impl OpusFrameQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, frame: Bytes) -> Result<(), Bytes> {
        if self.inner.len() >= self.capacity {
            return Err(frame);
        }

        self.inner.push_back(frame);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<Bytes> {
        self.inner.pop_front()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
