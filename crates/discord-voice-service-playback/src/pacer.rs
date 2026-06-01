use std::time::Duration;

use tokio::time::{self, Instant};

pub const FRAME_DURATION: Duration = Duration::from_millis(20);
pub const SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];

pub struct AudioPacer {
    next_deadline: Instant,
    emitted_frames: usize,
}

impl Default for AudioPacer {
    fn default() -> Self {
        Self::new()
    }
}

impl AudioPacer {
    pub fn new() -> Self {
        Self {
            next_deadline: Instant::now(),
            emitted_frames: 0,
        }
    }

    pub async fn wait_next(&mut self) {
        self.wait_for(FRAME_DURATION).await;
    }

    pub async fn wait_for(&mut self, duration: Duration) {
        time::sleep_until(self.next_deadline).await;
        self.next_deadline += duration;
        self.emitted_frames += 1;
    }

    pub async fn tick(&mut self) {
        self.wait_next().await;
    }

    pub fn emitted_frames(&self) -> usize {
        self.emitted_frames
    }
}
