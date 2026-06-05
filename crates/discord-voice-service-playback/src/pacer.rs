use std::time::Duration;

use tokio::time::{self, Instant};

pub const FRAME_DURATION: Duration = Duration::from_millis(20);
pub const SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];
const TIMER_WAKEUP_GUARD: Duration = Duration::from_millis(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacedPacketKind {
    Track,
    NonTrack,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PacerMark {
    pub media_clock_reset: bool,
    pub tempo_rebased: bool,
}

pub struct AudioPacer {
    ideal_deadline: Instant,
    minimum_deadline: Instant,
    emitted_frames: usize,
    clock_reset_count: usize,
    tempo_rebase_count: usize,
}

impl Default for AudioPacer {
    fn default() -> Self {
        Self::new()
    }
}

impl AudioPacer {
    pub fn new() -> Self {
        let deadline = Instant::now();
        Self {
            ideal_deadline: deadline,
            minimum_deadline: deadline,
            emitted_frames: 0,
            clock_reset_count: 0,
            tempo_rebase_count: 0,
        }
    }

    pub fn starting_after(delay: Duration) -> Self {
        let deadline = Instant::now() + delay;
        Self {
            ideal_deadline: deadline,
            minimum_deadline: deadline,
            emitted_frames: 0,
            clock_reset_count: 0,
            tempo_rebase_count: 0,
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
        sleep_until_precise(self.ideal_deadline).await;
        let effective_deadline = self.next_deadline();
        if Instant::now() < effective_deadline {
            sleep_until_precise(effective_deadline).await;
        }
    }

    pub fn next_deadline(&self) -> Instant {
        self.ideal_deadline.max(self.minimum_deadline)
    }

    pub fn mark_emitted(&mut self, duration: Duration) {
        let scheduled_deadline = self.next_deadline();
        self.mark_sent(
            PacedPacketKind::Track,
            scheduled_deadline,
            duration,
            Instant::now(),
        );
    }

    pub fn mark_sent(
        &mut self,
        packet_kind: PacedPacketKind,
        scheduled_deadline: Instant,
        duration: Duration,
        send_started_at: Instant,
    ) -> PacerMark {
        let lateness = send_started_at
            .checked_duration_since(scheduled_deadline)
            .unwrap_or(Duration::ZERO);
        let media_clock_reset = duration > Duration::ZERO && lateness >= duration;
        if media_clock_reset {
            self.reset_after_interruption_at(send_started_at, duration);
        } else {
            let next_deadline = send_started_at + duration;
            self.ideal_deadline = next_deadline;
            self.minimum_deadline = next_deadline;
        }

        self.emitted_frames += 1;
        let tempo_rebased = media_clock_reset && packet_kind == PacedPacketKind::Track;
        if tempo_rebased {
            self.tempo_rebase_count += 1;
        }
        PacerMark {
            media_clock_reset,
            tempo_rebased,
        }
    }

    pub fn reset_deadline(&mut self) {
        self.reset_after_interruption_at(Instant::now(), Duration::ZERO);
    }

    pub fn reset_after_interruption_at(&mut self, now: Instant, next_spacing: Duration) {
        let deadline = now + next_spacing;
        self.ideal_deadline = deadline;
        self.minimum_deadline = deadline;
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

    pub fn tempo_rebase_count(&self) -> usize {
        self.tempo_rebase_count
    }
}

async fn sleep_until_precise(deadline: Instant) {
    let now = Instant::now();
    if let Some(guarded_deadline) = deadline.checked_sub(TIMER_WAKEUP_GUARD)
        && guarded_deadline > now
    {
        time::sleep_until(guarded_deadline).await;
    }

    let spin_budget = deadline
        .saturating_duration_since(Instant::now())
        .saturating_add(TIMER_WAKEUP_GUARD);
    let spin_started_at = std::time::Instant::now();
    while Instant::now() < deadline {
        if spin_started_at.elapsed() >= spin_budget {
            time::sleep_until(deadline).await;
            break;
        }
        tokio::task::yield_now().await;
    }
}
