use std::time::Duration;

use tokio::time::{self, Instant};

pub const FRAME_DURATION: Duration = Duration::from_millis(20);
pub const MIN_RTP_SEND_SPACING: Duration = Duration::from_millis(8);
pub const SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];

pub struct AudioPacer {
    next_deadline: Instant,
    emitted_frames: usize,
    clock_reset_count: usize,
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
            clock_reset_count: 0,
        }
    }

    pub fn starting_after(delay: Duration) -> Self {
        Self {
            next_deadline: Instant::now() + delay,
            emitted_frames: 0,
            clock_reset_count: 0,
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

    pub fn next_deadline(&self) -> Instant {
        self.next_deadline
    }

    pub fn mark_emitted(&mut self, duration: Duration) {
        let scheduled_deadline = self.next_deadline;
        self.mark_sent(scheduled_deadline, duration, Instant::now());
    }

    pub fn mark_sent(
        &mut self,
        scheduled_deadline: Instant,
        duration: Duration,
        sent_at: Instant,
    ) -> bool {
        let lateness = sent_at
            .checked_duration_since(scheduled_deadline)
            .unwrap_or(Duration::ZERO);
        if lateness >= duration {
            self.reset_after_interruption_at(sent_at, duration);
            self.emitted_frames += 1;
            return true;
        }

        let next_by_schedule = scheduled_deadline + duration;
        let next_by_spacing = sent_at + MIN_RTP_SEND_SPACING;
        self.next_deadline = next_by_schedule.max(next_by_spacing);
        self.emitted_frames += 1;
        false
    }

    pub fn reset_deadline(&mut self) {
        self.reset_after_interruption_at(Instant::now(), Duration::ZERO);
    }

    pub fn reset_after_interruption_at(&mut self, now: Instant, next_spacing: Duration) {
        self.next_deadline = now + next_spacing;
        self.clock_reset_count += 1;
    }

    pub async fn tick(&mut self) {
        self.wait_next().await;
    }

    pub fn emitted_frames(&self) -> usize {
        self.emitted_frames
    }

    pub fn clock_reset_count(&self) -> usize {
        self.clock_reset_count
    }
}
