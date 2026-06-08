use std::pin::Pin;
use std::sync::Arc;

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service_proto::discordvoice::v1::join_voice_request;
use discord_voice_service_proto::discordvoice::v1::{
    DurationStatsSnapshot as ProtoDurationStatsSnapshot, GetPlaybackMetricsRequest,
    GetStateRequest, JoinVoiceRequest, JoinVoiceResponse, LeaveVoiceRequest, LeaveVoiceResponse,
    PauseRequest, PauseResponse, PlayRequest, PlayResponse,
    PlaybackBufferDepthSnapshot as ProtoPlaybackBufferDepthSnapshot,
    PlaybackQueueDepthStatsSnapshot as ProtoPlaybackQueueDepthStatsSnapshot,
    PlaybackSendCommandKind as ProtoPlaybackSendCommandKind,
    PlaybackSendEventSnapshot as ProtoPlaybackSendEventSnapshot,
    PlaybackStabilitySnapshot as ProtoPlaybackStabilitySnapshot,
    PreparedPlayoutQueueEventKind as ProtoPreparedPlayoutQueueEventKind,
    PreparedPlayoutQueueEventReason as ProtoPreparedPlayoutQueueEventReason,
    PreparedPlayoutQueueEventSnapshot as ProtoPreparedPlayoutQueueEventSnapshot,
    PreparedTrackQueueDepthSampleSnapshot as ProtoPreparedTrackQueueDepthSampleSnapshot,
    PreparedTrackQueueSamplePhase as ProtoPreparedTrackQueueSamplePhase, ResumeRequest,
    ResumeResponse, SessionEvent, SessionState as ProtoSessionState, SessionStateSnapshot,
    StopRequest, StopResponse, SubscribeEventsRequest, UpdateVoiceContextRequest,
    UpdateVoiceContextResponse,
};
use discord_voice_service_voice::VoiceContext;
use futures::{Stream, stream};
use tonic::{Request, Response, Status};

use crate::session::events::{SessionEventKind, SessionEventRecord};
use crate::{
    Command, DurationStatsSnapshot as RuntimeDurationStatsSnapshot,
    PlaybackBufferDepthSnapshot as RuntimePlaybackBufferDepthSnapshot,
    PlaybackQueueDepthStatsSnapshot as RuntimePlaybackQueueDepthStatsSnapshot,
    PlaybackSendCommandKind as RuntimePlaybackSendCommandKind,
    PlaybackSendEventSnapshot as RuntimePlaybackSendEventSnapshot,
    PlaybackStabilitySnapshot as RuntimePlaybackStabilitySnapshot,
    PreparedPlayoutQueueEventKind as RuntimePreparedPlayoutQueueEventKind,
    PreparedPlayoutQueueEventReason as RuntimePreparedPlayoutQueueEventReason,
    PreparedPlayoutQueueEventSnapshot as RuntimePreparedPlayoutQueueEventSnapshot,
    PreparedTrackQueueDepthSampleSnapshot as RuntimePreparedTrackQueueDepthSampleSnapshot,
    PreparedTrackQueueSamplePhase as RuntimePreparedTrackQueueSamplePhase, Readiness, SessionState,
    Supervisor, observability,
};

pub struct ControlService {
    pub supervisor: Supervisor,
    pub readiness: Arc<Readiness>,
}

#[tonic::async_trait]
impl DiscordVoiceControl for ControlService {
    type SubscribeEventsStream =
        Pin<Box<dyn Stream<Item = Result<SessionEvent, Status>> + Send + 'static>>;

    async fn join_voice(
        &self,
        request: Request<JoinVoiceRequest>,
    ) -> Result<Response<JoinVoiceResponse>, Status> {
        let voice = request
            .into_inner()
            .voice
            .ok_or_else(|| Status::invalid_argument("missing voice context"))
            .and_then(map_voice_context);
        let voice = observe_early_status("join_voice", voice)?;

        let result = self
            .supervisor
            .send(Command::JoinVoice { voice })
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("join_voice", &result);
        result?;

        Ok(Response::new(JoinVoiceResponse {}))
    }

    async fn update_voice_context(
        &self,
        request: Request<UpdateVoiceContextRequest>,
    ) -> Result<Response<UpdateVoiceContextResponse>, Status> {
        let voice = request
            .into_inner()
            .voice
            .ok_or_else(|| Status::invalid_argument("missing voice context"))
            .and_then(map_voice_context);
        let voice = observe_early_status("update_voice_context", voice)?;

        let result = self
            .supervisor
            .send(Command::UpdateVoiceContext { voice })
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("update_voice_context", &result);
        result?;
        Ok(Response::new(UpdateVoiceContextResponse {}))
    }

    async fn play(&self, request: Request<PlayRequest>) -> Result<Response<PlayResponse>, Status> {
        let request = request.into_inner();
        observe_early_status("play", validate_non_empty("video_id", &request.video_id))?;

        let result = self
            .supervisor
            .send(Command::Play {
                video_id: request.video_id,
            })
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("play", &result);
        result?;

        Ok(Response::new(PlayResponse {}))
    }

    async fn pause(
        &self,
        _request: Request<PauseRequest>,
    ) -> Result<Response<PauseResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::Pause)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("pause", &result);
        result?;
        Ok(Response::new(PauseResponse {}))
    }

    async fn resume(
        &self,
        _request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::Resume)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("resume", &result);
        result?;
        Ok(Response::new(ResumeResponse {}))
    }

    async fn stop(&self, _request: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::Stop)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("stop", &result);
        result?;
        Ok(Response::new(StopResponse {}))
    }

    async fn leave_voice(
        &self,
        _request: Request<LeaveVoiceRequest>,
    ) -> Result<Response<LeaveVoiceResponse>, Status> {
        let result = self
            .supervisor
            .send(Command::LeaveVoice)
            .await
            .map_err(map_app_error);
        observability::global().record_rpc_result("leave_voice", &result);
        result?;
        Ok(Response::new(LeaveVoiceResponse {}))
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<SessionStateSnapshot>, Status> {
        let snapshot = self.supervisor.snapshot().await;
        let readiness = self.readiness.snapshot().await;
        observability::global().record_state_query(&snapshot, readiness);
        observability::global().record_rpc("get_state", tonic::Code::Ok);
        Ok(Response::new(SessionStateSnapshot {
            state: map_session_state(snapshot.state) as i32,
            guild_id: snapshot.guild_id.unwrap_or_default(),
            channel_id: snapshot.channel_id.unwrap_or_default(),
            current_video_id: snapshot.current_video_id.unwrap_or_default(),
            queue_depth: u32::try_from(snapshot.queue_depth).unwrap_or(u32::MAX),
            selected_itag: snapshot.selected_itag.unwrap_or_default(),
            message: snapshot.last_reason.unwrap_or_default(),
        }))
    }

    async fn get_playback_metrics(
        &self,
        _request: Request<GetPlaybackMetricsRequest>,
    ) -> Result<Response<ProtoPlaybackStabilitySnapshot>, Status> {
        let snapshot = self.supervisor.playback_metrics().await;
        observability::global().record_rpc("get_playback_metrics", tonic::Code::Ok);
        Ok(Response::new(map_playback_stability_snapshot(snapshot)))
    }

    async fn subscribe_events(
        &self,
        _request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let rx = self.supervisor.subscribe_events();
        let stream = stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => return Some((Ok(map_session_event(event)), rx)),
                    // Broadcast channels drop the oldest retained events for lagging
                    // receivers; continue from the oldest event still available.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

fn map_playback_stability_snapshot(
    snapshot: Option<RuntimePlaybackStabilitySnapshot>,
) -> ProtoPlaybackStabilitySnapshot {
    let Some(snapshot) = snapshot else {
        return ProtoPlaybackStabilitySnapshot {
            available: false,
            ..Default::default()
        };
    };

    ProtoPlaybackStabilitySnapshot {
        available: true,
        playback_epoch: snapshot.playback_epoch,
        video_id: snapshot.video_id.unwrap_or_default(),
        selected_itag: snapshot.selected_itag.unwrap_or_default(),
        track_packet_count: usize_to_u64(snapshot.track_packet_count),
        continuity_silence_packet_count: usize_to_u64(snapshot.continuity_silence_packet_count),
        inserted_silence_duration_ms: snapshot.inserted_silence_duration_ms,
        track_interval: Some(map_duration_stats(snapshot.track_interval)),
        track_media_duration_sent_ms: snapshot.track_media_duration_sent_ms,
        track_wall_clock_elapsed_ms: snapshot.track_wall_clock_elapsed_ms,
        track_media_to_wall_clock_ratio_ppm: snapshot.track_media_to_wall_clock_ratio_ppm,
        track_fast_interval_count: snapshot.track_fast_interval_count,
        track_fast_interval_min_ms: snapshot.track_fast_interval_min_ms,
        track_fast_interval_min_us: snapshot.track_fast_interval_min_us,
        track_tempo_window_count: snapshot.track_tempo_window_count,
        track_tempo_window_post_source_buffer_count: snapshot
            .track_tempo_window_post_source_buffer_count,
        track_tempo_window_min_ratio_ppm: snapshot.track_tempo_window_min_ratio_ppm,
        track_tempo_window_max_ratio_ppm: snapshot.track_tempo_window_max_ratio_ppm,
        track_tempo_window_fast_count: snapshot.track_tempo_window_fast_count,
        track_tempo_window_fastest_ratio_ppm: snapshot.track_tempo_window_fastest_ratio_ppm,
        track_tempo_window_fastest_media_ms: snapshot.track_tempo_window_fastest_media_ms,
        track_tempo_window_fastest_wall_clock_us: snapshot.track_tempo_window_fastest_wall_clock_us,
        track_tempo_window_slow_count: snapshot.track_tempo_window_slow_count,
        track_tempo_window_slowest_ratio_ppm: snapshot.track_tempo_window_slowest_ratio_ppm,
        track_tempo_window_slowest_media_ms: snapshot.track_tempo_window_slowest_media_ms,
        track_tempo_window_slowest_wall_clock_us: snapshot.track_tempo_window_slowest_wall_clock_us,
        skipped_source_frame_count: snapshot.skipped_source_frame_count,
        skipped_source_duration_ms: snapshot.skipped_source_duration_ms,
        tempo_rebase_count: snapshot.tempo_rebase_count,
        expected_track_frame_count: snapshot.expected_track_frame_count,
        sent_track_frame_count: snapshot.sent_track_frame_count,
        silence_frame_count: snapshot.silence_frame_count,
        frame_deficit_count: snapshot.frame_deficit_count,
        dropped_frame_count: snapshot.dropped_frame_count,
        late_frame_count: snapshot.late_frame_count,
        all_packet_interval: Some(map_duration_stats(snapshot.all_packet_interval)),
        sender_lateness: Some(map_duration_stats(snapshot.sender_lateness)),
        max_consecutive_late_packets: usize_to_u64(snapshot.max_consecutive_late_packets),
        current_consecutive_late_packets: usize_to_u64(snapshot.current_consecutive_late_packets),
        current_buffer_depth: Some(map_buffer_depth(snapshot.current_buffer_depth)),
        min_buffer_depth: Some(map_buffer_depth(snapshot.min_buffer_depth)),
        max_buffer_depth: Some(map_buffer_depth(snapshot.max_buffer_depth)),
        current_source_buffer_depth: Some(map_buffer_depth(snapshot.current_source_buffer_depth)),
        min_source_buffer_depth: Some(map_buffer_depth(snapshot.min_source_buffer_depth)),
        max_source_buffer_depth: Some(map_buffer_depth(snapshot.max_source_buffer_depth)),
        source_buffer_depth: Some(map_queue_depth_stats(snapshot.source_buffer_depth)),
        current_playout_buffer_depth: Some(map_buffer_depth(snapshot.current_playout_buffer_depth)),
        min_playout_buffer_depth: Some(map_buffer_depth(snapshot.min_playout_buffer_depth)),
        max_playout_buffer_depth: Some(map_buffer_depth(snapshot.max_playout_buffer_depth)),
        egress_buffer_target_ms: snapshot.egress_buffer_target_ms,
        current_egress_buffer_depth: Some(map_buffer_depth(snapshot.current_egress_buffer_depth)),
        min_egress_buffer_depth: Some(map_buffer_depth(snapshot.min_egress_buffer_depth)),
        max_egress_buffer_depth: Some(map_buffer_depth(snapshot.max_egress_buffer_depth)),
        prepared_rtp_queue_depth_ms: snapshot.prepared_rtp_queue_depth_ms,
        prepared_track_queue_target_ms: snapshot.prepared_track_queue_target_ms,
        prepared_track_queue_low_watermark_ms: snapshot.prepared_track_queue_low_watermark_ms,
        prepared_track_queue_high_watermark_ms: snapshot.prepared_track_queue_high_watermark_ms,
        active_pre_pause_prepared_track_queue_depth: Some(map_queue_depth_stats(
            snapshot.active_pre_pause_prepared_track_queue_depth,
        )),
        active_post_resume_prepared_track_queue_depth: Some(map_queue_depth_stats(
            snapshot.active_post_resume_prepared_track_queue_depth,
        )),
        prepared_track_queue_depth_sample_count: usize_to_u64(
            snapshot.prepared_track_queue_depth_sample_count,
        ),
        prepared_track_queue_empty_count: snapshot.prepared_track_queue_empty_count,
        raw_send_events: snapshot
            .raw_send_events
            .into_iter()
            .map(map_raw_send_event)
            .collect(),
        raw_prepared_track_queue_samples: snapshot
            .raw_prepared_track_queue_samples
            .into_iter()
            .map(map_raw_prepared_track_queue_sample)
            .collect(),
        raw_prepared_playout_queue_events: snapshot
            .raw_prepared_playout_queue_events
            .into_iter()
            .map(map_raw_prepared_playout_queue_event)
            .collect(),
        current_scheduled_silence_queue_depth: Some(map_buffer_depth(
            snapshot.current_scheduled_silence_queue_depth,
        )),
        max_scheduled_silence_queue_depth: Some(map_buffer_depth(
            snapshot.max_scheduled_silence_queue_depth,
        )),
        current_boundary_queue_depth: Some(map_buffer_depth(snapshot.current_boundary_queue_depth)),
        max_boundary_queue_depth: Some(map_buffer_depth(snapshot.max_boundary_queue_depth)),
        prepared_track_packet_drop_count: snapshot.prepared_track_packet_drop_count,
        prepared_silence_packet_drop_count: snapshot.prepared_silence_packet_drop_count,
        prepared_packet_rebuild_count: snapshot.prepared_packet_rebuild_count,
        scheduled_silence_packet_count: snapshot.scheduled_silence_packet_count,
        pause_media_boundary_count: snapshot.pause_media_boundary_count,
        stop_media_boundary_count: snapshot.stop_media_boundary_count,
        recovery_media_boundary_count: snapshot.recovery_media_boundary_count,
        natural_end_media_boundary_count: snapshot.natural_end_media_boundary_count,
        dave_transition_recovery_reached_builder_count: snapshot
            .dave_transition_recovery_reached_builder_count,
        dave_transition_recovery_reached_deadline_sender_count: snapshot
            .dave_transition_recovery_reached_deadline_sender_count,
        source_underrun_reached_builder_count: snapshot.source_underrun_reached_builder_count,
        source_underrun_reached_deadline_sender_count: snapshot
            .source_underrun_reached_deadline_sender_count,
        discarded_source_frame_count: snapshot.discarded_source_frame_count,
        discarded_source_duration_ms: snapshot.discarded_source_duration_ms,
        stop_discarded_source_frame_count: snapshot.stop_discarded_source_frame_count,
        stop_discarded_source_duration_ms: snapshot.stop_discarded_source_duration_ms,
        interruption_discarded_source_frame_count: snapshot
            .interruption_discarded_source_frame_count,
        interruption_discarded_source_duration_ms: snapshot
            .interruption_discarded_source_duration_ms,
        restored_source_frame_count: snapshot.restored_source_frame_count,
        restored_source_duration_ms: snapshot.restored_source_duration_ms,
        source_buffer_target_ms: snapshot.source_buffer_target_ms,
        adaptive_buffer_target_ms: snapshot.adaptive_buffer_target_ms,
        max_adaptive_buffer_target_ms: snapshot.max_adaptive_buffer_target_ms,
        buffer_low_watermark_count: snapshot.buffer_low_watermark_count,
        source_buffer_low_watermark_count: snapshot.source_buffer_low_watermark_count,
        playout_buffer_low_watermark_count: snapshot.playout_buffer_low_watermark_count,
        buffer_underrun_count: snapshot.buffer_underrun_count,
        playout_underrun_count: snapshot.playout_underrun_count,
        egress_underrun_count: snapshot.egress_underrun_count,
        egress_inserted_silence_duration_ms: snapshot.egress_inserted_silence_duration_ms,
        egress_dropped_music_frame_count: snapshot.egress_dropped_music_frame_count,
        egress_dropped_music_duration_ms: snapshot.egress_dropped_music_duration_ms,
        rebuffer_count: snapshot.rebuffer_count,
        refill_duration: Some(map_duration_stats(snapshot.refill_duration)),
        producer_stall_duration: Some(map_duration_stats(snapshot.producer_stall_duration)),
        max_producer_lag_ms: snapshot.max_producer_lag_ms,
        http_retry_count: snapshot.http_retry_count,
        response_open_count: snapshot.response_open_count,
        range_reopen_count: snapshot.range_reopen_count,
        read_error_reopen_count: snapshot.read_error_reopen_count,
        url_reresolve_count: snapshot.url_reresolve_count,
        pause_resume_first_intervals_ms: snapshot.pause_resume_first_intervals_ms,
        post_stall_first_intervals_ms: snapshot.post_stall_first_intervals_ms,
        post_rebuffer_first_intervals_ms: snapshot.post_rebuffer_first_intervals_ms,
        playout_sender_lateness: Some(map_duration_stats(snapshot.playout_sender_lateness)),
        max_consecutive_playout_late_packets: usize_to_u64(
            snapshot.max_consecutive_playout_late_packets,
        ),
        max_consecutive_late_egress_ticks: usize_to_u64(snapshot.max_consecutive_late_egress_ticks),
        speaking_prepare_duration: Some(map_duration_stats(snapshot.speaking_prepare_duration)),
        source_underrun_count: snapshot.source_underrun_count,
        source_producer_fill_duration: Some(map_duration_stats(
            snapshot.source_producer_fill_duration,
        )),
        playout_builder_prepare_duration: Some(map_duration_stats(
            snapshot.playout_builder_prepare_duration,
        )),
        sender_send_duration: Some(map_duration_stats(snapshot.sender_send_duration)),
        sender_loop_non_send_work_duration: Some(map_duration_stats(
            snapshot.sender_loop_non_send_work_duration,
        )),
        sender_forbidden_work_count: snapshot.sender_forbidden_work_count,
        gateway_event_drain_duration: Some(map_duration_stats(
            snapshot.gateway_event_drain_duration,
        )),
        gateway_event_drain_count: snapshot.gateway_event_drain_count,
        dave_transition_count: snapshot.dave_transition_count,
        dave_transition_count_during_playback: snapshot.dave_transition_count_during_playback,
        stale_dave_send_prevented_count: snapshot.stale_dave_send_prevented_count,
        controlled_media_interruption_count: snapshot.controlled_media_interruption_count,
        media_clock_reset_count: snapshot.media_clock_reset_count,
        egress_clock_reset_count: snapshot.egress_clock_reset_count,
        scheduler_late_reset_count: snapshot.scheduler_late_reset_count,
        source_underrun_reset_count: snapshot.source_underrun_reset_count,
        pause_resume_reset_count: snapshot.pause_resume_reset_count,
        dave_transition_recovery_reset_count: snapshot.dave_transition_recovery_reset_count,
        gateway_interruptions: snapshot.gateway_interruptions,
        dave_interruptions: snapshot.dave_interruptions,
        reconnect_interruptions: snapshot.reconnect_interruptions,
        ended: snapshot.ended,
    }
}

fn map_duration_stats(snapshot: RuntimeDurationStatsSnapshot) -> ProtoDurationStatsSnapshot {
    ProtoDurationStatsSnapshot {
        samples: usize_to_u64(snapshot.samples),
        p50_ms: snapshot.p50_ms,
        p95_ms: snapshot.p95_ms,
        p99_ms: snapshot.p99_ms,
        min_ms: snapshot.min_ms,
        max_ms: snapshot.max_ms,
    }
}

fn map_buffer_depth(
    snapshot: RuntimePlaybackBufferDepthSnapshot,
) -> ProtoPlaybackBufferDepthSnapshot {
    ProtoPlaybackBufferDepthSnapshot {
        packets: usize_to_u64(snapshot.packets),
        bytes: usize_to_u64(snapshot.bytes),
        duration_ms: snapshot.duration_ms,
        duration_samples: snapshot.duration_samples,
    }
}

fn map_queue_depth_stats(
    snapshot: RuntimePlaybackQueueDepthStatsSnapshot,
) -> ProtoPlaybackQueueDepthStatsSnapshot {
    ProtoPlaybackQueueDepthStatsSnapshot {
        sample_count: usize_to_u64(snapshot.sample_count),
        empty_count: snapshot.empty_count,
        current_depth: Some(map_buffer_depth(snapshot.current_depth)),
        min_depth: Some(map_buffer_depth(snapshot.min_depth)),
        p5_depth: Some(map_buffer_depth(snapshot.p5_depth)),
        p50_depth: Some(map_buffer_depth(snapshot.p50_depth)),
        p95_depth: Some(map_buffer_depth(snapshot.p95_depth)),
        max_depth: Some(map_buffer_depth(snapshot.max_depth)),
    }
}

fn map_raw_send_event(event: RuntimePlaybackSendEventSnapshot) -> ProtoPlaybackSendEventSnapshot {
    ProtoPlaybackSendEventSnapshot {
        packet_index: event.packet_index,
        command_kind: map_send_command_kind(event.command_kind) as i32,
        expected_deadline_offset_us: event.expected_deadline_offset_us,
        send_started_offset_us: event.send_started_offset_us,
        sent_offset_us: event.sent_offset_us,
        media_duration_ms: event.media_duration_ms,
        media_duration_samples: event.media_duration_samples,
        rtp_sequence: u32::from(event.rtp_sequence),
        rtp_timestamp: event.rtp_timestamp,
        protection_nonce: event.protection_nonce,
        source_frame_epoch: event.source_frame_epoch,
        source_media_position_ms: event.source_media_position_ms,
        source_media_byte_position: event.source_media_byte_position,
        committed_heard_media: event.committed_heard_media,
    }
}

fn map_send_command_kind(kind: RuntimePlaybackSendCommandKind) -> ProtoPlaybackSendCommandKind {
    match kind {
        RuntimePlaybackSendCommandKind::Track => ProtoPlaybackSendCommandKind::Track,
        RuntimePlaybackSendCommandKind::ScheduledSilence => {
            ProtoPlaybackSendCommandKind::ScheduledSilence
        }
        RuntimePlaybackSendCommandKind::BoundarySilence => {
            ProtoPlaybackSendCommandKind::BoundarySilence
        }
        RuntimePlaybackSendCommandKind::OtherBoundary => {
            ProtoPlaybackSendCommandKind::OtherBoundary
        }
    }
}

fn map_raw_prepared_track_queue_sample(
    sample: RuntimePreparedTrackQueueDepthSampleSnapshot,
) -> ProtoPreparedTrackQueueDepthSampleSnapshot {
    ProtoPreparedTrackQueueDepthSampleSnapshot {
        sample_index: sample.sample_index,
        phase: map_prepared_track_queue_sample_phase(sample.phase) as i32,
        depth: Some(map_buffer_depth(sample.depth)),
    }
}

fn map_raw_prepared_playout_queue_event(
    event: RuntimePreparedPlayoutQueueEventSnapshot,
) -> ProtoPreparedPlayoutQueueEventSnapshot {
    ProtoPreparedPlayoutQueueEventSnapshot {
        event_index: event.event_index,
        event_kind: map_prepared_playout_queue_event_kind(event.event_kind) as i32,
        reason: map_prepared_playout_queue_event_reason(event.reason) as i32,
        command_kind: map_send_command_kind(event.command_kind) as i32,
        media_duration_ms: event.media_duration_ms,
        media_duration_samples: event.media_duration_samples,
        rtp_sequence: event.rtp_sequence,
        rtp_timestamp: event.rtp_timestamp,
        protection_nonce: event.protection_nonce,
        source_frame_epoch: event.source_frame_epoch,
        source_media_position_ms: event.source_media_position_ms,
        source_media_byte_position: event.source_media_byte_position,
        queue_depth_after: Some(map_buffer_depth(event.queue_depth_after)),
    }
}

fn map_prepared_playout_queue_event_kind(
    event_kind: RuntimePreparedPlayoutQueueEventKind,
) -> ProtoPreparedPlayoutQueueEventKind {
    match event_kind {
        RuntimePreparedPlayoutQueueEventKind::Unknown => {
            ProtoPreparedPlayoutQueueEventKind::Unspecified
        }
        RuntimePreparedPlayoutQueueEventKind::Enqueued => {
            ProtoPreparedPlayoutQueueEventKind::Enqueued
        }
        RuntimePreparedPlayoutQueueEventKind::DequeuedToDeadlineSender => {
            ProtoPreparedPlayoutQueueEventKind::DequeuedToDeadlineSender
        }
        RuntimePreparedPlayoutQueueEventKind::DroppedBeforeSend => {
            ProtoPreparedPlayoutQueueEventKind::DroppedBeforeSend
        }
        RuntimePreparedPlayoutQueueEventKind::Rebuilt => {
            ProtoPreparedPlayoutQueueEventKind::Rebuilt
        }
    }
}

fn map_prepared_playout_queue_event_reason(
    reason: RuntimePreparedPlayoutQueueEventReason,
) -> ProtoPreparedPlayoutQueueEventReason {
    match reason {
        RuntimePreparedPlayoutQueueEventReason::Unspecified => {
            ProtoPreparedPlayoutQueueEventReason::Unspecified
        }
        RuntimePreparedPlayoutQueueEventReason::SteadyPlayback => {
            ProtoPreparedPlayoutQueueEventReason::SteadyPlayback
        }
        RuntimePreparedPlayoutQueueEventReason::Pause => {
            ProtoPreparedPlayoutQueueEventReason::Pause
        }
        RuntimePreparedPlayoutQueueEventReason::Stop => ProtoPreparedPlayoutQueueEventReason::Stop,
        RuntimePreparedPlayoutQueueEventReason::DaveTransitionRecovery => {
            ProtoPreparedPlayoutQueueEventReason::DaveTransitionRecovery
        }
        RuntimePreparedPlayoutQueueEventReason::Reconnect => {
            ProtoPreparedPlayoutQueueEventReason::Reconnect
        }
        RuntimePreparedPlayoutQueueEventReason::SourceUnderrun => {
            ProtoPreparedPlayoutQueueEventReason::SourceUnderrun
        }
        RuntimePreparedPlayoutQueueEventReason::NaturalEnd => {
            ProtoPreparedPlayoutQueueEventReason::NaturalEnd
        }
        RuntimePreparedPlayoutQueueEventReason::Interruption => {
            ProtoPreparedPlayoutQueueEventReason::Interruption
        }
    }
}

fn map_prepared_track_queue_sample_phase(
    phase: RuntimePreparedTrackQueueSamplePhase,
) -> ProtoPreparedTrackQueueSamplePhase {
    match phase {
        RuntimePreparedTrackQueueSamplePhase::ActivePrePause => {
            ProtoPreparedTrackQueueSamplePhase::ActivePrePause
        }
        RuntimePreparedTrackQueueSamplePhase::ActivePostResume => {
            ProtoPreparedTrackQueueSamplePhase::ActivePostResume
        }
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn map_app_error(error: crate::error::RuntimeError) -> Status {
    Status::failed_precondition(error.to_string())
}

#[allow(clippy::result_large_err)]
fn observe_early_status<T>(method: &'static str, result: Result<T, Status>) -> Result<T, Status> {
    match result {
        Ok(value) => Ok(value),
        Err(status) => {
            observability::global().record_rpc(method, status.code());
            Err(status)
        }
    }
}

#[allow(clippy::result_large_err)]
fn validate_non_empty(field: &'static str, value: &str) -> Result<(), Status> {
    if value.trim().is_empty() {
        Err(Status::invalid_argument(format!("{field} is required")))
    } else {
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
fn map_voice_context(voice: join_voice_request::VoiceContext) -> Result<VoiceContext, Status> {
    validate_non_empty("guild_id", &voice.guild_id)?;
    validate_non_empty("channel_id", &voice.channel_id)?;
    validate_non_empty("user_id", &voice.user_id)?;
    validate_non_empty("session_id", &voice.session_id)?;
    validate_non_empty("endpoint", &voice.endpoint)?;
    validate_non_empty("token", &voice.token)?;

    Ok(VoiceContext {
        guild_id: voice.guild_id,
        channel_id: voice.channel_id,
        user_id: voice.user_id,
        session_id: voice.session_id,
        endpoint: voice.endpoint,
        token: voice.token,
    })
}

fn map_session_state(state: SessionState) -> ProtoSessionState {
    match state {
        SessionState::Idle => ProtoSessionState::Idle,
        SessionState::ConnectingVoice => ProtoSessionState::ConnectingVoice,
        SessionState::VoiceReady => ProtoSessionState::VoiceReadyState,
        SessionState::ResolvingTrack => ProtoSessionState::ResolvingTrack,
        SessionState::Buffering => ProtoSessionState::BufferingState,
        SessionState::Playing => ProtoSessionState::PlayingState,
        SessionState::Paused => ProtoSessionState::PausedState,
        SessionState::Stopping => ProtoSessionState::Stopping,
        SessionState::Error => ProtoSessionState::ErrorState,
    }
}

fn map_session_event(event: SessionEventRecord) -> SessionEvent {
    SessionEvent {
        kind: map_session_event_kind(event.kind) as i32,
        guild_id: event.guild_id.unwrap_or_default(),
        channel_id: event.channel_id.unwrap_or_default(),
        current_video_id: event.current_video_id.unwrap_or_default(),
        selected_itag: event.selected_itag.unwrap_or_default(),
        message: event.message.unwrap_or_default(),
        reason: discord_voice_service_proto::discordvoice::v1::SessionEventReason::Unspecified
            as i32,
        ..Default::default()
    }
}

fn map_session_event_kind(
    kind: SessionEventKind,
) -> discord_voice_service_proto::discordvoice::v1::SessionEventKind {
    match kind {
        SessionEventKind::VoiceConnecting => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::VoiceConnecting
        }
        SessionEventKind::VoiceReady => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::VoiceReady
        }
        SessionEventKind::TrackResolving => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::TrackResolving
        }
        SessionEventKind::Buffering => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::Buffering
        }
        SessionEventKind::Playing => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::Playing
        }
        SessionEventKind::Paused => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::Paused
        }
        SessionEventKind::Stopped => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::Stopped
        }
        SessionEventKind::TrackEnded => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::TrackEnded
        }
        SessionEventKind::PlaybackInterrupted => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::PlaybackInterrupted
        }
        SessionEventKind::RecoverableWarning => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::RecoverableWarning
        }
        SessionEventKind::FatalError => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::FatalError
        }
        SessionEventKind::VoiceReconnecting => {
            discord_voice_service_proto::discordvoice::v1::SessionEventKind::VoiceReconnecting
        }
    }
}
