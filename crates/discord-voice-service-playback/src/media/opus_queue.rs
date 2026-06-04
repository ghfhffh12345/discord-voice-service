use std::collections::VecDeque;

use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct OpusFrame {
    pub data: Bytes,
    pub duration_ms: u64,
    pub duration_samples: u32,
    pub source_byte_position: Option<u64>,
    pub epoch: u64,
}

impl OpusFrame {
    pub fn new(data: Bytes, duration_ms: u64) -> Self {
        let duration_samples = samples_from_duration_ms(duration_ms);
        Self {
            data,
            duration_ms,
            duration_samples,
            source_byte_position: None,
            epoch: 0,
        }
    }

    pub fn with_duration_samples(data: Bytes, duration_ms: u64, duration_samples: u32) -> Self {
        Self {
            data,
            duration_ms,
            duration_samples,
            source_byte_position: None,
            epoch: 0,
        }
    }

    pub fn with_metadata(mut self, source_byte_position: Option<u64>, epoch: u64) -> Self {
        self.source_byte_position = source_byte_position;
        self.epoch = epoch;
        self
    }

    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn byte_len(&self) -> usize {
        self.data.len()
    }
}

impl PartialEq for OpusFrame {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
            && self.duration_ms == other.duration_ms
            && self.duration_samples == other.duration_samples
            && self.source_byte_position == other.source_byte_position
            && self.epoch == other.epoch
    }
}

impl Eq for OpusFrame {}

pub fn samples_from_duration_ms(duration_ms: u64) -> u32 {
    duration_ms
        .saturating_mul(48)
        .try_into()
        .unwrap_or(u32::MAX)
}

#[derive(Debug)]
pub struct OpusFrameQueue {
    capacity: usize,
    max_bytes: usize,
    max_duration_ms: u64,
    duration_ms: u64,
    duration_samples: u64,
    bytes: usize,
    inner: VecDeque<OpusFrame>,
}

impl OpusFrameQueue {
    pub fn new(capacity: usize) -> Self {
        Self::with_limits(capacity, usize::MAX)
    }

    pub fn with_limits(capacity: usize, max_bytes: usize) -> Self {
        Self::with_resource_limits(capacity, max_bytes, u64::MAX)
    }

    pub fn with_resource_limits(capacity: usize, max_bytes: usize, max_duration_ms: u64) -> Self {
        Self {
            capacity,
            max_bytes,
            max_duration_ms,
            duration_ms: 0,
            duration_samples: 0,
            bytes: 0,
            inner: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, frame: OpusFrame) -> Result<(), OpusFrame> {
        if self.inner.len() >= self.capacity {
            return Err(frame);
        }
        if self.bytes.saturating_add(frame.byte_len()) > self.max_bytes {
            return Err(frame);
        }
        if self.duration_ms.saturating_add(frame.duration_ms) > self.max_duration_ms {
            return Err(frame);
        }

        self.duration_ms = self.duration_ms.saturating_add(frame.duration_ms);
        self.duration_samples = self
            .duration_samples
            .saturating_add(u64::from(frame.duration_samples));
        self.bytes = self.bytes.saturating_add(frame.byte_len());
        self.inner.push_back(frame);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<OpusFrame> {
        let frame = self.inner.pop_front()?;
        self.duration_ms = self.duration_ms.saturating_sub(frame.duration_ms);
        self.duration_samples = self
            .duration_samples
            .saturating_sub(u64::from(frame.duration_samples));
        self.bytes = self.bytes.saturating_sub(frame.byte_len());
        Some(frame)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.inner.len() >= self.capacity
            || self.bytes >= self.max_bytes
            || self.duration_ms >= self.max_duration_ms
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn buffered_duration_ms(&self) -> u64 {
        self.duration_ms
    }

    pub fn buffered_samples(&self) -> u64 {
        self.duration_samples
    }

    pub fn buffered_bytes(&self) -> usize {
        self.bytes
    }

    pub fn depth(&self) -> OpusBufferDepth {
        OpusBufferDepth {
            packets: self.len(),
            bytes: self.buffered_bytes(),
            duration_ms: self.buffered_duration_ms(),
            duration_samples: self.buffered_samples(),
        }
    }

    pub fn drop_stale_epoch(&mut self, epoch: u64) {
        let mut retained = VecDeque::with_capacity(self.capacity);
        while let Some(frame) = self.inner.pop_front() {
            if frame.epoch == epoch {
                retained.push_back(frame);
            }
        }

        self.inner = retained;
        self.recalculate_depth();
    }

    pub fn retag_epoch(&mut self, epoch: u64) {
        for frame in &mut self.inner {
            frame.epoch = epoch;
        }
    }

    fn recalculate_depth(&mut self) {
        self.duration_ms = 0;
        self.duration_samples = 0;
        self.bytes = 0;
        for frame in &self.inner {
            self.duration_ms = self.duration_ms.saturating_add(frame.duration_ms);
            self.duration_samples = self
                .duration_samples
                .saturating_add(u64::from(frame.duration_samples));
            self.bytes = self.bytes.saturating_add(frame.byte_len());
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OpusBufferDepth {
    pub packets: usize,
    pub bytes: usize,
    pub duration_ms: u64,
    pub duration_samples: u64,
}
