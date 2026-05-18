use std::time::Duration;

pub const FRAME_DURATION: Duration = Duration::from_millis(20);
pub const SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];
