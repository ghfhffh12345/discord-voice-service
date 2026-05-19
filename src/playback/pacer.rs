use std::time::Duration;

use tokio::time::{self, Interval, MissedTickBehavior};

pub const FRAME_DURATION: Duration = Duration::from_millis(20);
pub const SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];

pub struct AudioPacer {
    ticker: Interval,
    emitted_frames: usize,
}

impl AudioPacer {
    pub fn new() -> Self {
        let mut ticker = time::interval(FRAME_DURATION);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            ticker,
            emitted_frames: 0,
        }
    }

    pub async fn wait_next(&mut self) {
        self.ticker.tick().await;
        self.emitted_frames += 1;
    }

    pub async fn tick(&mut self) {
        self.wait_next().await;
    }

    pub fn emitted_frames(&self) -> usize {
        self.emitted_frames
    }
}
