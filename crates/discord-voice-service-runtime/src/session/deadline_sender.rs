use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use discord_voice_service_playback::media::opus_queue::duration_from_samples;
use discord_voice_service_voice::{PreparedVoicePacket, VoicePreparedPacketSender};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::{self, Instant};

use crate::RuntimeError;

pub(crate) const DEADLINE_TICK: Duration = Duration::from_millis(20);
const TIMER_WAKEUP_GUARD: Duration = Duration::from_millis(3);

pub(crate) trait PreparedUdpSink {
    fn send_prepared_packet<'a>(
        &'a mut self,
        packet: &'a PreparedVoicePacket,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>>;
}

impl PreparedUdpSink for &mut VoicePreparedPacketSender {
    fn send_prepared_packet<'a>(
        &'a mut self,
        packet: &'a PreparedVoicePacket,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            (**self)
                .send_prepared_packet(packet)
                .await
                .map_err(Into::into)
        })
    }
}

impl PreparedUdpSink for VoicePreparedPacketSender {
    fn send_prepared_packet<'a>(
        &'a mut self,
        packet: &'a PreparedVoicePacket,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move { self.send_prepared_packet(packet).await.map_err(Into::into) })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparedPacketKind {
    Track,
    ScheduledSilence,
    BoundarySilence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedMediaFrame {
    pub(crate) duration_ms: u64,
    pub(crate) duration_samples: u32,
    pub(crate) media_position_ms: u64,
    pub(crate) media_position_samples: u64,
    pub(crate) media_byte_position: Option<u64>,
    pub(crate) epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedPlayoutCommand {
    pub(crate) packet: PreparedVoicePacket,
    pub(crate) kind: PreparedPacketKind,
    pub(crate) media_frame: Option<PreparedMediaFrame>,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeadlineSendRecord {
    pub(crate) expected_deadline: Instant,
    pub(crate) send_started_at: Instant,
    pub(crate) sent_at: Instant,
    pub(crate) kind: PreparedPacketKind,
    pub(crate) duration_ms: u64,
    pub(crate) duration_samples: u32,
    pub(crate) rtp_sequence: u16,
    pub(crate) rtp_timestamp: u32,
    pub(crate) protection_nonce: Option<u32>,
    pub(crate) media_frame: Option<PreparedMediaFrame>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeadlineDropRecord {
    pub(crate) kind: PreparedPacketKind,
    pub(crate) duration_ms: u64,
    pub(crate) duration_samples: u32,
    pub(crate) rtp_sequence: u16,
    pub(crate) rtp_timestamp: u32,
    pub(crate) protection_nonce: Option<u32>,
    pub(crate) media_frame: Option<PreparedMediaFrame>,
    pub(crate) generation: u64,
}

impl DeadlineDropRecord {
    fn from_command(command: &PreparedPlayoutCommand) -> Self {
        Self {
            kind: command.kind,
            duration_ms: command.packet.duration_ms,
            duration_samples: command.packet.duration_samples,
            rtp_sequence: command.packet.rtp_sequence,
            rtp_timestamp: command.packet.rtp_timestamp,
            protection_nonce: command.packet.protection_nonce,
            media_frame: command.media_frame,
            generation: command.generation,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct DeadlineSenderMetrics {
    sent: Vec<DeadlineSendRecord>,
    empty_tick_count: u64,
}

impl DeadlineSenderMetrics {
    #[cfg(test)]
    pub(crate) fn sent(&self) -> &[DeadlineSendRecord] {
        &self.sent
    }

    #[cfg(test)]
    pub(crate) fn empty_tick_count(&self) -> u64 {
        self.empty_tick_count
    }

    fn record_sent(
        &mut self,
        expected_deadline: Instant,
        send_started_at: Instant,
        sent_at: Instant,
        command: &PreparedPlayoutCommand,
    ) -> DeadlineSendRecord {
        let record = DeadlineSendRecord {
            expected_deadline,
            send_started_at,
            sent_at,
            kind: command.kind,
            duration_ms: command.packet.duration_ms,
            duration_samples: command.packet.duration_samples,
            rtp_sequence: command.packet.rtp_sequence,
            rtp_timestamp: command.packet.rtp_timestamp,
            protection_nonce: command.packet.protection_nonce,
            media_frame: command.media_frame,
        };
        self.sent.push(record);
        record
    }

    fn record_empty_tick(&mut self) {
        self.empty_tick_count = self.empty_tick_count.saturating_add(1);
    }
}

pub(crate) struct DeadlineSender<S> {
    sink: S,
    commands: mpsc::Receiver<PreparedPlayoutCommand>,
    send_records: Option<mpsc::Sender<DeadlineSendRecord>>,
    drop_records: Option<mpsc::Sender<DeadlineDropRecord>>,
    active_generation: Option<Arc<AtomicU64>>,
    shutdown: Arc<AtomicBool>,
    metrics: DeadlineSenderMetrics,
    tick: Duration,
    next_deadline: Option<Instant>,
}

impl<S> DeadlineSender<S>
where
    S: PreparedUdpSink,
{
    #[cfg(test)]
    pub(crate) fn new(
        sink: S,
        commands: mpsc::Receiver<PreparedPlayoutCommand>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            sink,
            commands,
            send_records: None,
            drop_records: None,
            active_generation: None,
            shutdown,
            metrics: DeadlineSenderMetrics::default(),
            tick: DEADLINE_TICK,
            next_deadline: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_next_deadline(
        sink: S,
        commands: mpsc::Receiver<PreparedPlayoutCommand>,
        shutdown: Arc<AtomicBool>,
        next_deadline: Instant,
    ) -> Self {
        Self {
            sink,
            commands,
            send_records: None,
            drop_records: None,
            active_generation: None,
            shutdown,
            metrics: DeadlineSenderMetrics::default(),
            tick: DEADLINE_TICK,
            next_deadline: Some(next_deadline),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_next_deadline_and_records(
        sink: S,
        commands: mpsc::Receiver<PreparedPlayoutCommand>,
        send_records: mpsc::Sender<DeadlineSendRecord>,
        shutdown: Arc<AtomicBool>,
        next_deadline: Instant,
    ) -> Self {
        Self {
            sink,
            commands,
            send_records: Some(send_records),
            drop_records: None,
            active_generation: None,
            shutdown,
            metrics: DeadlineSenderMetrics::default(),
            tick: DEADLINE_TICK,
            next_deadline: Some(next_deadline),
        }
    }

    pub(crate) fn new_with_next_deadline_records_and_generation(
        sink: S,
        commands: mpsc::Receiver<PreparedPlayoutCommand>,
        send_records: mpsc::Sender<DeadlineSendRecord>,
        drop_records: mpsc::Sender<DeadlineDropRecord>,
        active_generation: Arc<AtomicU64>,
        shutdown: Arc<AtomicBool>,
        next_deadline: Instant,
    ) -> Self {
        Self {
            sink,
            commands,
            send_records: Some(send_records),
            drop_records: Some(drop_records),
            active_generation: Some(active_generation),
            shutdown,
            metrics: DeadlineSenderMetrics::default(),
            tick: DEADLINE_TICK,
            next_deadline: Some(next_deadline),
        }
    }

    pub(crate) async fn run(mut self) -> Result<DeadlineSenderMetrics, RuntimeError> {
        while !self.shutdown.load(Ordering::Relaxed) {
            self.tick_once().await?;
        }
        Ok(self.metrics)
    }

    #[cfg(test)]
    pub(crate) async fn run_for_ticks(
        mut self,
        ticks: usize,
    ) -> Result<DeadlineSenderMetrics, RuntimeError> {
        for _ in 0..ticks {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            self.tick_once().await?;
        }
        Ok(self.metrics)
    }

    async fn tick_once(&mut self) -> Result<(), RuntimeError> {
        let expected_deadline = self
            .next_deadline
            .take()
            .unwrap_or_else(|| Instant::now() + self.tick);
        if expected_deadline > Instant::now() {
            sleep_until_precise(expected_deadline).await;
        }

        let command = match self.commands.try_recv() {
            Ok(command) => command,
            Err(TryRecvError::Empty) => {
                self.metrics.record_empty_tick();
                self.next_deadline = Some(Instant::now() + self.tick);
                return Ok(());
            }
            Err(TryRecvError::Disconnected) => {
                self.shutdown.store(true, Ordering::Relaxed);
                return Ok(());
            }
        };
        if let Some(active_generation) = &self.active_generation {
            let active_generation = active_generation.load(Ordering::Acquire);
            if command.generation != active_generation {
                if let Some(drop_records) = &self.drop_records {
                    drop_records
                        .try_send(DeadlineDropRecord::from_command(&command))
                        .map_err(|_| {
                            RuntimeError::InvalidState("deadline drop record channel unavailable")
                        })?;
                }
                self.next_deadline = Some(Instant::now() + self.tick);
                return Ok(());
            }
        }

        let send_started_at = Instant::now();
        self.sink.send_prepared_packet(&command.packet).await?;
        let sent_at = Instant::now();
        let send_interval = packet_send_interval(&command.packet);
        let ideal_next_deadline = send_started_at + send_interval;
        self.next_deadline = Some(if sent_at >= ideal_next_deadline {
            sent_at + send_interval
        } else {
            ideal_next_deadline
        });
        let record =
            self.metrics
                .record_sent(expected_deadline, send_started_at, sent_at, &command);
        if let Some(send_records) = &self.send_records {
            send_records.try_send(record).map_err(|_| {
                RuntimeError::InvalidState("deadline send record channel unavailable")
            })?;
        }
        Ok(())
    }
}

fn packet_send_interval(packet: &PreparedVoicePacket) -> Duration {
    duration_from_samples(u64::from(packet.duration_samples)).max(Duration::from_millis(1))
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Mutex;
    use tokio::time;

    #[derive(Default)]
    struct RecordingSink {
        sent: Arc<Mutex<Vec<(u16, Instant)>>>,
        send_delay: Duration,
    }

    impl RecordingSink {
        fn with_delay(send_delay: Duration) -> Self {
            Self {
                sent: Arc::default(),
                send_delay,
            }
        }
    }

    impl PreparedUdpSink for RecordingSink {
        fn send_prepared_packet<'a>(
            &'a mut self,
            packet: &'a PreparedVoicePacket,
        ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
            Box::pin(async move {
                if !self.send_delay.is_zero() {
                    time::sleep(self.send_delay).await;
                }
                self.sent
                    .lock()
                    .expect("recording sink lock should not be poisoned")
                    .push((packet.rtp_sequence, Instant::now()));
                Ok(())
            })
        }
    }

    fn prepared_command(sequence: u16, timestamp: u32) -> PreparedPlayoutCommand {
        prepared_command_with_duration(sequence, timestamp, 20, 960)
    }

    fn prepared_command_with_duration(
        sequence: u16,
        timestamp: u32,
        duration_ms: u64,
        duration_samples: u32,
    ) -> PreparedPlayoutCommand {
        PreparedPlayoutCommand {
            packet: PreparedVoicePacket {
                bytes: Bytes::from(vec![sequence as u8]),
                duration_ms,
                duration_samples,
                is_track: true,
                rtp_sequence: sequence,
                rtp_timestamp: timestamp,
                protection_nonce: None,
            },
            kind: PreparedPacketKind::Track,
            media_frame: Some(PreparedMediaFrame {
                duration_ms,
                duration_samples,
                media_position_ms: u64::from(sequence) * duration_ms,
                media_position_samples: u64::from(sequence) * u64::from(duration_samples),
                media_byte_position: None,
                epoch: 1,
            }),
            generation: 0,
        }
    }

    fn sender_with_commands(
        commands: Vec<PreparedPlayoutCommand>,
        sink: RecordingSink,
    ) -> (
        DeadlineSender<RecordingSink>,
        Arc<Mutex<Vec<(u16, Instant)>>>,
    ) {
        let sent = Arc::clone(&sink.sent);
        let (tx, rx) = mpsc::channel(16);
        for command in commands {
            tx.try_send(command)
                .expect("test command queue should have capacity");
        }
        drop(tx);
        (
            DeadlineSender::new(sink, rx, Arc::new(AtomicBool::new(false))),
            sent,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_sends_one_prepared_packet_per_twenty_ms_tick() {
        let commands = (0..3)
            .map(|index| prepared_command(index, u32::from(index) * 960))
            .collect::<Vec<_>>();
        let (sender, sent) = sender_with_commands(commands, RecordingSink::default());

        let metrics = sender.run_for_ticks(3).await.unwrap();

        let sent = sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone();
        assert_eq!(
            sent.iter()
                .map(|(sequence, _)| *sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let intervals = sent
            .windows(2)
            .map(|window| window[1].1 - window[0].1)
            .collect::<Vec<_>>();
        assert_eq!(intervals, vec![DEADLINE_TICK, DEADLINE_TICK]);
        assert_eq!(metrics.empty_tick_count(), 0);
        assert_eq!(metrics.sent().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_schedules_next_send_from_prepared_packet_duration() {
        let commands = vec![
            prepared_command_with_duration(0, 0, 60, 2_880),
            prepared_command_with_duration(1, 2_880, 20, 960),
            prepared_command_with_duration(2, 3_840, 20, 960),
        ];
        let (sender, sent) = sender_with_commands(commands, RecordingSink::default());

        let metrics = sender.run_for_ticks(3).await.unwrap();

        let sent = sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone();
        let intervals = sent
            .windows(2)
            .map(|window| window[1].1 - window[0].1)
            .collect::<Vec<_>>();
        assert_eq!(intervals, vec![Duration::from_millis(60), DEADLINE_TICK]);
        assert_eq!(metrics.empty_tick_count(), 0);
        assert_eq!(
            metrics
                .sent()
                .iter()
                .map(|record| record.duration_ms)
                .collect::<Vec<_>>(),
            vec![60, 20, 20]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_schedules_fractional_millisecond_packet_from_samples() {
        let commands = vec![
            prepared_command_with_duration(0, 0, 2, 120),
            prepared_command_with_duration(1, 120, 2, 120),
            prepared_command_with_duration(2, 240, 20, 960),
        ];
        let (sender, sent) = sender_with_commands(commands, RecordingSink::default());

        let metrics = sender.run_for_ticks(3).await.unwrap();

        let sent = sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone();
        let actual_intervals = sent
            .windows(2)
            .map(|window| window[1].1 - window[0].1)
            .collect::<Vec<_>>();
        assert_eq!(
            actual_intervals,
            vec![Duration::from_millis(3), Duration::from_millis(3)]
        );
        let scheduled_intervals = metrics
            .sent()
            .windows(2)
            .map(|window| window[1].expected_deadline - window[0].sent_at)
            .collect::<Vec<_>>();
        assert_eq!(
            scheduled_intervals,
            vec![Duration::from_micros(2_500), Duration::from_micros(2_500)]
        );
        assert_eq!(
            metrics
                .sent()
                .iter()
                .map(|record| (record.duration_ms, record.duration_samples))
                .collect::<Vec<_>>(),
            vec![(2, 120), (2, 120), (20, 960)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_schedules_mixed_duration_sequence_from_samples() {
        let commands = vec![
            prepared_command_with_duration(0, 0, 20, 960),
            prepared_command_with_duration(1, 960, 2, 120),
            prepared_command_with_duration(2, 1_080, 7, 360),
            prepared_command_with_duration(3, 1_440, 5, 240),
        ];
        let (sender, sent) = sender_with_commands(commands, RecordingSink::default());

        let metrics = sender.run_for_ticks(4).await.unwrap();

        let sent = sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone();
        let actual_intervals = sent
            .windows(2)
            .map(|window| window[1].1 - window[0].1)
            .collect::<Vec<_>>();
        assert_eq!(
            actual_intervals,
            vec![
                Duration::from_millis(20),
                Duration::from_millis(3),
                Duration::from_millis(8),
            ]
        );
        let scheduled_intervals = metrics
            .sent()
            .windows(2)
            .map(|window| window[1].expected_deadline - window[0].sent_at)
            .collect::<Vec<_>>();
        assert_eq!(
            scheduled_intervals,
            vec![
                Duration::from_millis(20),
                Duration::from_micros(2_500),
                Duration::from_micros(7_500),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_preserves_media_cadence_through_small_send_latency() {
        let commands = (0..3)
            .map(|index| prepared_command(index, u32::from(index) * 960))
            .collect::<Vec<_>>();
        let (sender, sent) = sender_with_commands(
            commands,
            RecordingSink::with_delay(Duration::from_millis(5)),
        );

        let metrics = sender.run_for_ticks(3).await.unwrap();

        let sent = sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone();
        let completed_send_intervals = sent
            .windows(2)
            .map(|window| window[1].1 - window[0].1)
            .collect::<Vec<_>>();
        assert_eq!(completed_send_intervals, vec![DEADLINE_TICK; 2]);

        let started_send_intervals = metrics
            .sent()
            .windows(2)
            .map(|window| window[1].send_started_at - window[0].send_started_at)
            .collect::<Vec<_>>();
        assert_eq!(started_send_intervals, vec![DEADLINE_TICK; 2]);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_does_not_emit_catch_up_burst_after_slow_send() {
        let commands = (0..3)
            .map(|index| prepared_command(index, u32::from(index) * 960))
            .collect::<Vec<_>>();
        let (sender, sent) = sender_with_commands(
            commands,
            RecordingSink::with_delay(Duration::from_millis(65)),
        );

        sender.run_for_ticks(3).await.unwrap();

        let sent = sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone();
        let intervals = sent
            .windows(2)
            .map(|window| window[1].1 - window[0].1)
            .collect::<Vec<_>>();
        assert_eq!(
            intervals,
            vec![DEADLINE_TICK + Duration::from_millis(65); 2]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_empty_queue_is_nonblocking_and_does_not_drain_later_backlog() {
        let (tx, rx) = mpsc::channel(16);
        let sink = RecordingSink::default();
        let sent = Arc::clone(&sink.sent);
        let sender = DeadlineSender::new(sink, rx, Arc::new(AtomicBool::new(false)));

        tx.try_send(prepared_command(0, 0))
            .expect("first command should enqueue");
        let metrics = sender.run_for_ticks(3).await.unwrap();

        let sent = sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(metrics.empty_tick_count(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_exits_when_prepared_command_channel_closes() {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let sender = DeadlineSender::new_with_next_deadline(
            RecordingSink::default(),
            rx,
            Arc::new(AtomicBool::new(false)),
            Instant::now(),
        );

        let metrics = sender.run().await.unwrap();

        assert_eq!(metrics.sent(), &[]);
        assert_eq!(metrics.empty_tick_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_emits_send_commit_records_without_broad_runtime_access() {
        let (command_tx, command_rx) = mpsc::channel(16);
        command_tx
            .try_send(prepared_command(0, 0))
            .expect("prepared command should enqueue");
        command_tx
            .try_send(prepared_command(1, 960))
            .expect("prepared command should enqueue");
        drop(command_tx);

        let (record_tx, mut record_rx) = mpsc::channel(16);
        let sender = DeadlineSender::new_with_next_deadline_and_records(
            RecordingSink::default(),
            command_rx,
            record_tx,
            Arc::new(AtomicBool::new(false)),
            Instant::now() + DEADLINE_TICK,
        );

        let metrics = sender.run_for_ticks(2).await.unwrap();
        let first = record_rx
            .try_recv()
            .expect("first send commit record should be available");
        let second = record_rx
            .try_recv()
            .expect("second send commit record should be available");

        assert_eq!(metrics.sent(), &[first, second]);
        assert_eq!(first.kind, PreparedPacketKind::Track);
        assert_eq!(first.duration_ms, 20);
        assert_eq!(first.duration_samples, 960);
        assert_eq!(first.rtp_sequence, 0);
        assert_eq!(first.rtp_timestamp, 0);
        assert_eq!(
            first.media_frame,
            Some(PreparedMediaFrame {
                duration_ms: 20,
                duration_samples: 960,
                media_position_ms: 0,
                media_position_samples: 0,
                media_byte_position: None,
                epoch: 1,
            })
        );
        assert_eq!(second.rtp_sequence, 1);
        assert_eq!(second.rtp_timestamp, 960);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_sender_drops_stale_generation_with_media_identity() {
        let (command_tx, command_rx) = mpsc::channel(16);
        let mut stale = prepared_command(0, 0);
        stale.generation = 1;
        let mut current = prepared_command(1, 960);
        current.generation = 2;
        command_tx
            .try_send(stale)
            .expect("stale prepared command should enqueue");
        command_tx
            .try_send(current)
            .expect("current prepared command should enqueue");
        drop(command_tx);

        let active_generation = Arc::new(AtomicU64::new(2));
        let (send_tx, mut send_rx) = mpsc::channel(16);
        let (drop_tx, mut drop_rx) = mpsc::channel(16);
        let sender = DeadlineSender::new_with_next_deadline_records_and_generation(
            RecordingSink::default(),
            command_rx,
            send_tx,
            drop_tx,
            Arc::clone(&active_generation),
            Arc::new(AtomicBool::new(false)),
            Instant::now() + DEADLINE_TICK,
        );

        sender.run_for_ticks(2).await.unwrap();

        let dropped = drop_rx
            .try_recv()
            .expect("stale command should produce a drop record");
        assert_eq!(dropped.generation, 1);
        assert_eq!(dropped.rtp_sequence, 0);
        assert_eq!(
            dropped.media_frame,
            Some(PreparedMediaFrame {
                duration_ms: 20,
                duration_samples: 960,
                media_position_ms: 0,
                media_position_samples: 0,
                media_byte_position: None,
                epoch: 1,
            })
        );
        let sent = send_rx
            .try_recv()
            .expect("current generation should still send");
        assert_eq!(sent.rtp_sequence, 1);
        assert_eq!(
            sent.media_frame
                .expect("track media identity")
                .media_position_ms,
            20
        );
    }

    #[test]
    fn deadline_sender_source_does_not_import_forbidden_runtime_work() {
        let production_source = include_str!("deadline_sender.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("deadline sender module should include production source");
        for forbidden in [
            "source",
            "gateway",
            "dave",
            "recover",
            "demux",
            "ProtectionContext",
            "RtpPacketBuilder",
            "VoiceUdpTransport",
            "ConnectedVoiceSession",
            "LiveMediaDriver",
        ] {
            assert!(
                !production_source.contains(forbidden),
                "deadline sender must not import or mention forbidden work: {forbidden}"
            );
        }
    }

    #[test]
    fn deadline_sender_state_is_only_the_minimal_send_boundary_dependencies() {
        let production_source = include_str!("deadline_sender.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("deadline sender module should include production source");
        let state = production_source
            .split("pub(crate) struct DeadlineSender<S> {")
            .nth(1)
            .and_then(|tail| tail.split("}\n\nimpl<S> DeadlineSender<S>").next())
            .expect("deadline sender state should be visible in this module");

        for required_field in [
            "sink: S",
            "commands: mpsc::Receiver<PreparedPlayoutCommand>",
            "send_records: Option<mpsc::Sender<DeadlineSendRecord>>",
            "drop_records: Option<mpsc::Sender<DeadlineDropRecord>>",
            "active_generation: Option<Arc<AtomicU64>>",
            "shutdown: Arc<AtomicBool>",
            "metrics: DeadlineSenderMetrics",
            "tick: Duration",
            "next_deadline: Option<Instant>",
        ] {
            assert!(
                state.contains(required_field),
                "deadline sender should keep narrow dependency field: {required_field}"
            );
        }
        for forbidden in [
            "ConnectedVoiceSession",
            "VoiceUdpTransport",
            "VoicePacketPreparer",
            "RtpPacketBuilder",
            "ProtectionContext",
            "LiveMediaDriver",
            "Mutex",
            "RwLock",
            "watch::",
            "oneshot::",
        ] {
            assert!(
                !state.contains(forbidden),
                "deadline sender state must not retain forbidden dependency: {forbidden}"
            );
        }
    }

    #[test]
    fn deadline_sender_tick_has_only_timer_and_udp_send_awaits() {
        let production_source = include_str!("deadline_sender.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("deadline sender module should include production source");
        let tick_once = production_source
            .split("async fn tick_once")
            .nth(1)
            .and_then(|tail| tail.split("\n    }\n}").next())
            .expect("deadline sender tick should be visible in this module");

        assert_eq!(
            tick_once.matches(".await").count(),
            2,
            "deadline sender tick may await only the timer and prepared UDP send"
        );
        assert!(
            tick_once.contains("sleep_until_precise(expected_deadline).await"),
            "deadline sender tick should wait only for the next deadline"
        );
        assert!(
            tick_once.contains("self.sink.send_prepared_packet(&command.packet).await?"),
            "deadline sender tick should send only one already-prepared packet"
        );
        for forbidden in [
            ".recv().await",
            ".lock().await",
            "prepare_",
            "process_pending",
            "settle_pending",
            "discard_unsent",
            "reset_deadline",
        ] {
            assert!(
                !tick_once.contains(forbidden),
                "deadline sender tick must not reach forbidden work: {forbidden}"
            );
        }
    }
}
