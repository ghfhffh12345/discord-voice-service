use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;

pub const OPUS_SAMPLE_RATE_HZ: u64 = 48_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpusPacketDuration {
    pub ms: u64,
    pub samples: u32,
}

#[derive(Clone, Debug)]
pub struct OpusFrame {
    pub data: Bytes,
    pub duration_ms: u64,
    pub duration_samples: u32,
    pub source_position_ms: u64,
    pub source_position_samples: u64,
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
            source_position_ms: 0,
            source_position_samples: 0,
            source_byte_position: None,
            epoch: 0,
        }
    }

    pub fn with_duration_samples(data: Bytes, duration_ms: u64, duration_samples: u32) -> Self {
        Self {
            data,
            duration_ms,
            duration_samples,
            source_position_ms: 0,
            source_position_samples: 0,
            source_byte_position: None,
            epoch: 0,
        }
    }

    pub fn with_metadata(
        mut self,
        source_position_ms: u64,
        source_byte_position: Option<u64>,
        epoch: u64,
    ) -> Self {
        self.source_position_ms = source_position_ms;
        self.source_position_samples = samples_from_duration_ms_u64(source_position_ms);
        self.source_byte_position = source_byte_position;
        self.epoch = epoch;
        self
    }

    pub fn with_exact_metadata(
        mut self,
        source_position_ms: u64,
        source_position_samples: u64,
        source_byte_position: Option<u64>,
        epoch: u64,
    ) -> Self {
        self.source_position_ms = source_position_ms;
        self.source_position_samples = source_position_samples;
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
            && self.source_position_ms == other.source_position_ms
            && self.source_position_samples == other.source_position_samples
            && self.source_byte_position == other.source_byte_position
            && self.epoch == other.epoch
    }
}

impl Eq for OpusFrame {}

pub fn samples_from_duration_ms(duration_ms: u64) -> u32 {
    samples_from_duration_ms_u64(duration_ms)
        .try_into()
        .unwrap_or(u32::MAX)
}

pub fn samples_from_duration_ms_u64(duration_ms: u64) -> u64 {
    duration_ms.saturating_mul(OPUS_SAMPLE_RATE_HZ / 1_000)
}

pub fn duration_ms_from_samples(duration_samples: u64) -> u64 {
    duration_samples.saturating_mul(1_000) / OPUS_SAMPLE_RATE_HZ
}

pub fn duration_from_samples(duration_samples: u64) -> Duration {
    Duration::from_nanos(duration_samples.saturating_mul(1_000_000_000) / OPUS_SAMPLE_RATE_HZ)
}

pub fn opus_packet_duration(packet: &[u8]) -> Option<OpusPacketDuration> {
    let toc = *packet.first()?;
    let samples_per_frame = if (toc & 0x80) != 0 {
        let shift = usize::from((toc >> 3) & 0x03);
        (48_000usize << shift) / 400
    } else if (toc & 0x60) == 0x60 {
        if (toc & 0x08) != 0 {
            48_000usize / 50
        } else {
            48_000usize / 100
        }
    } else {
        let index = usize::from((toc >> 3) & 0x03);
        if index == 3 {
            48_000usize * 60 / 1_000
        } else {
            (48_000usize << index) / 100
        }
    };

    let frames = match toc & 0x03 {
        0 => 1usize,
        1 | 2 => 2usize,
        3 => usize::from(*packet.get(1)? & 0x3F),
        _ => return None,
    };

    let samples = samples_per_frame.checked_mul(frames)?.try_into().ok()?;
    Some(OpusPacketDuration {
        ms: duration_ms_from_samples(u64::from(samples)),
        samples,
    })
}

#[derive(Debug)]
pub struct OpusFrameQueue {
    capacity: usize,
    max_bytes: usize,
    max_duration_samples: u64,
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
            max_duration_samples: samples_from_duration_ms_u64(max_duration_ms),
            duration_samples: 0,
            bytes: 0,
            inner: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, frame: OpusFrame) -> Result<(), OpusFrame> {
        if !self.can_fit_frame(&frame) {
            return Err(frame);
        }

        self.duration_samples = self
            .duration_samples
            .saturating_add(u64::from(frame.duration_samples));
        self.bytes = self.bytes.saturating_add(frame.byte_len());
        self.inner.push_back(frame);
        Ok(())
    }

    pub fn push_front(&mut self, frame: OpusFrame) -> Result<(), OpusFrame> {
        if !self.can_fit_frame(&frame) {
            return Err(frame);
        }

        self.duration_samples = self
            .duration_samples
            .saturating_add(u64::from(frame.duration_samples));
        self.bytes = self.bytes.saturating_add(frame.byte_len());
        self.inner.push_front(frame);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<OpusFrame> {
        let frame = self.inner.pop_front()?;
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
            || self.duration_samples >= self.max_duration_samples
    }

    pub fn can_fit_frame(&self, frame: &OpusFrame) -> bool {
        self.can_fit_resource(frame.byte_len(), frame.duration_samples)
    }

    pub fn can_fit_resource(&self, byte_len: usize, duration_samples: u32) -> bool {
        self.inner.len() < self.capacity
            && self.bytes.saturating_add(byte_len) <= self.max_bytes
            && self
                .duration_samples
                .saturating_add(u64::from(duration_samples))
                <= self.max_duration_samples
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn buffered_duration_ms(&self) -> u64 {
        duration_ms_from_samples(self.duration_samples)
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
        self.duration_samples = 0;
        self.bytes = 0;
        for frame in &self.inner {
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
