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
        self.wait_until_ready().await;
        self.mark_emitted(duration);
    }

    pub async fn wait_until_ready(&self) {
        time::sleep_until(self.next_deadline).await;
    }

    pub fn mark_emitted(&mut self, duration: Duration) {
        self.next_deadline += duration;
        let now = Instant::now();
        if self.next_deadline < now {
            self.next_deadline = now + duration;
        }
        self.emitted_frames += 1;
    }

    pub fn reset_deadline(&mut self) {
        self.next_deadline = Instant::now();
    }

    pub async fn tick(&mut self) {
        self.wait_next().await;
    }

    pub fn emitted_frames(&self) -> usize {
        self.emitted_frames
    }
}
