use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout};
use tracing::{info, warn};
use twilight_gateway::error::ReceiveMessageErrorType;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_http::{Client as HttpClient, error::ErrorType as HttpErrorType};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, UserMarker},
};
use twilight_model::voice::VoiceState;

use discord_voice_service_playback::YtMusicClient;
use discord_voice_service_twilight::{
    Client as VoiceServiceClient, PlaybackBufferDepthSnapshot, PlaybackQueueDepthStatsSnapshot,
    PlaybackSendCommandKind, PlaybackStabilitySnapshot, PreparedPlayoutQueueEventKind,
    PreparedPlayoutQueueEventReason, PreparedTrackQueueSamplePhase, SessionEvent, SessionEventKind,
    SessionState, StateSnapshot, VoiceContext as ServiceVoiceContext, VoiceContextTracker,
    join_voice_channel, leave_voice_channel,
};
use discord_voice_service_voice::{
    ObservedAudioFrame, ObservedVoiceActivity, ObservedVoiceSession, PendingObservedVoiceSession,
    VoiceContext as ObserverVoiceContext, VoiceError,
};

use crate::audio::{
    AudioTempoWindowEvidence, AudioValidationAccumulator, AudioValidationStats, ObservedOpusPacket,
};
use crate::config::{
    LIVE_STAGING_PROFILE_CONSTRAINED_GITHUB, LIVE_STAGING_PROFILE_CONSTRAINED_LOCAL,
    MAX_LIVE_STAGING_SERVICE_CPUS, MIN_LIVE_STAGING_CPU_CONTENTION_WORKERS,
    MIN_LIVE_STAGING_HTTP_READ_DELAY_MS, MIN_LIVE_STAGING_HTTP_READ_JITTER_MS, StagingConfig,
};
use crate::contract::{
    LiveContractState, LiveValidationEvidence, PlaybackStabilityEvidence, emit_validation_evidence,
    finalize_success_evidence,
};

const AUTHENTIC_VOICE_EVENT_TIMEOUT: Duration = Duration::from_secs(45);
const GATEWAY_LEAVE_TIMEOUT: Duration = Duration::from_secs(30);
const GATEWAY_LEAVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_CONTRACT_TIMEOUT: Duration = Duration::from_secs(240);
const LIVE_INTERRUPT_PROBE_TIMEOUT: Duration = Duration::from_secs(60);
const INTERRUPT_PROBE_PLAY_TASK_POLL_INTERVAL: Duration = Duration::from_millis(100);
const PLAYBACK_METRICS_TIMEOUT: Duration = Duration::from_secs(5);
const PLAYBACK_METRICS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const MIN_LIVE_TEST_VIDEO_DURATION_MS: u64 = 90_000;
const ACTIVE_INTERRUPT_PROBE_MEDIA_SETTLE_DURATION: Duration = Duration::from_millis(500);
const MIN_STABILITY_METRIC_PACKET_COUNT: u64 = 50;
const SOURCE_PLAYBACK_BUFFER_TARGET_MS: u64 = 5_000;
const DISCORD_EGRESS_BUFFER_TARGET_MS: u64 = 400;
const DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS: u64 = 300;
const DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS: u64 = 500;
const PREPARED_TRACK_QUEUE_MIN_DEPTH_MS: u64 = 200;
const PREPARED_TRACK_QUEUE_P5_MIN_DEPTH_MS: u64 = 300;
const PREPARED_TRACK_QUEUE_P50_MIN_DEPTH_MS: u64 = 340;
const PREPARED_TRACK_QUEUE_P50_MAX_DEPTH_MS: u64 = 460;
const TRACK_TEMPO_WINDOW_PACKETS: usize = 50;
#[cfg(test)]
const NATURAL_OPUS_FRAME_DURATION_MS: u64 = 20;
#[cfg(test)]
const NATURAL_OPUS_FRAME_DURATION_SAMPLES: u32 = 960;
const MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS: u64 = 500;
const SOURCE_POSITION_CONTINUITY_TOLERANCE_MS: u64 = 1;
const RAW_RATIO_RECOMPUTE_TOLERANCE_PPM: u64 = 2;
const RTP_INTERVAL_P95_BUDGET_MS: u64 = 45;
const RTP_INTERVAL_P99_BUDGET_MS: u64 = 70;
const RTP_INTERVAL_MAX_BUDGET_MS: u64 = 200;
const MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM: u64 = 980_000;
const MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM: u64 = 1_020_000;
const OBSERVER_SHORT_WINDOW_MIN_RATIO_PPM: u64 = 940_000;
const OBSERVER_SHORT_WINDOW_MAX_RATIO_PPM: u64 = 1_060_000;
const OBSERVER_STRICT_SHORT_WINDOW_MIN_PACKETS: u64 = 50;
const OBSERVER_MICRO_WINDOW_MIN_RATIO_PPM: u64 = 900_000;
const OBSERVER_MICRO_WINDOW_MAX_RATIO_PPM: u64 = 1_120_000;
const OBSERVER_SPEED_CHANGE_TOTAL_ABS_MIN_BUDGET_US: u64 = 250_000;
const OBSERVER_SPEED_CHANGE_TOTAL_ABS_MAX_BUDGET_US: u64 = 1_000_000;
const OBSERVER_SPEED_CHANGE_TOTAL_ABS_RATIO_DENOMINATOR: u64 = 200;
const SENDER_LATENESS_P99_BUDGET_MS: u64 = 10;
const SENDER_LOOP_NON_SEND_WORK_P99_BUDGET_MS: u64 = 15;
const MAX_CONSECUTIVE_PLAYOUT_LATE_PACKETS: u64 = 2;
const MIN_OBSERVED_PACKET_COUNT: u64 = 120;
const MIN_DECODED_AUDIO_MS: u64 = 6_000;
const MIN_NON_SILENT_AUDIO_MS: u64 = 1_000;
const OBSERVER_AUDIO_DURATION_TOLERANCE_MS: u64 = 2_000;
const OBSERVER_AUDIO_DURATION_MIN_RATIO_PERCENT: u64 = 90;
const OBSERVER_TRACK_ENDED_DRAIN_GRACE: Duration = Duration::from_millis(750);
const OBSERVER_AUDIO_STARTED_TIMEOUT: Duration = Duration::from_secs(30);
const PAUSE_OBSERVER_SILENCE_DURATION: Duration = Duration::from_millis(600);
const PAUSE_AFTER_PLAYBACK_START: Duration = Duration::from_secs(10);
const PAUSE_HOLD_DURATION: Duration = Duration::from_secs(3);
const PAUSE_STOP_SILENCE_FRAME_COUNT: usize = 5;
const PAUSE_PROOF_ARM_POLL_INTERVAL: Duration = Duration::from_millis(5);
const PAUSE_BOUNDARY_MIN_SPACING_MS: u64 = 15;
const PAUSE_BOUNDARY_MAX_SPACING_MS: u64 = 45;
const OPUS_STOP_SILENCE_FRAME: [u8; 3] = [0xF8, 0xFF, 0xFE];
const OBSERVER_SPEAKING_STATE_TIMEOUT: Duration = Duration::from_secs(10);
const RESUME_OBSERVER_AUDIO_TIMEOUT: Duration = Duration::from_secs(10);
const RESUME_OBSERVER_PACKET_TARGET: u64 = 4;
const SPEAKING_FLAG_MICROPHONE: u64 = 1;

fn to_observer_voice_context(context: &ServiceVoiceContext) -> ObserverVoiceContext {
    ObserverVoiceContext {
        guild_id: context.guild_id.to_string(),
        channel_id: context.channel_id.to_string(),
        user_id: context.user_id.to_string(),
        session_id: context.session_id.clone(),
        endpoint: context.endpoint.clone(),
        token: context.token.clone(),
    }
}

struct LiveGatewayDriverConfig {
    service_addr: String,
    initial_service_voice: ServiceVoiceContext,
    guild_id: Id<GuildMarker>,
    observer_user_id: Id<UserMarker>,
}

#[derive(Debug)]
struct ValidatedLiveOutcome {
    live_contract: LiveContractState,
    audio_stats: AudioValidationStats,
    observer_playback: ObserverPlaybackProof,
    expected_duration_ms: u64,
    playback_metrics: Option<PlaybackStabilityEvidence>,
    reconnect_probe_metrics: Option<PlaybackStabilityEvidence>,
}

#[derive(Debug, Clone, Default)]
struct ObserverPlaybackProof {
    pause_silence_ms: u64,
    pause_self_mute_observed: bool,
    pause_speaking_stopped: bool,
    pause_rtp_silence_observed: bool,
    resume_speaking_started: bool,
    resume_observed_packet_count: u64,
    resume_decoded_audio_start_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct FailureEvidenceSnapshot {
    live_contract: Option<LiveContractState>,
    audio_stats: Option<AudioValidationStats>,
    observer_playback: Option<ObserverPlaybackProof>,
    playback_metrics: Option<PlaybackStabilityEvidence>,
    reconnect_probe_metrics: Option<PlaybackStabilityEvidence>,
}

#[derive(Debug)]
struct ServiceFlowOutcome {
    result: Result<ValidatedLiveOutcome>,
    failure_snapshot: FailureEvidenceSnapshot,
    service_joined: bool,
}

#[derive(Debug)]
struct CompletedPostPlayControlEvidence {
    playback_metrics: PlaybackStabilityEvidence,
    reconnect_probe_metrics: PlaybackStabilityEvidence,
}

#[derive(Debug, Clone, Default)]
struct PostPlayControlEvidence {
    playback_metrics: Option<PlaybackStabilityEvidence>,
    reconnect_probe_metrics: Option<PlaybackStabilityEvidence>,
}

#[derive(Debug)]
struct PostPlayControlFailure {
    error: anyhow::Error,
    evidence: PostPlayControlEvidence,
}

impl PostPlayControlEvidence {
    fn fail<T>(self, error: anyhow::Error) -> std::result::Result<T, Box<PostPlayControlFailure>> {
        Err(Box::new(PostPlayControlFailure {
            error,
            evidence: self,
        }))
    }
}

impl FailureEvidenceSnapshot {
    fn record_post_play_evidence(&mut self, evidence: PostPlayControlEvidence) {
        if evidence.playback_metrics.is_some() {
            self.playback_metrics = evidence.playback_metrics;
        }
        if evidence.reconnect_probe_metrics.is_some() {
            self.reconnect_probe_metrics = evidence.reconnect_probe_metrics;
        }
    }
}

#[derive(Clone, Copy)]
struct PostPlayProbeConfig<'a> {
    expected_video_id: &'a str,
}

#[derive(Debug)]
struct ObserverPauseProof {
    silence_ms: u64,
    gateway_speaking_stopped: bool,
    rtp_silence_observed: bool,
}

#[derive(Debug)]
struct ObserverResumeProof {
    observed_packet_count: u64,
    speaking_started: bool,
    resume_decoded_audio_start_ms: u64,
}

struct PendingObserverPauseProof {
    begin: Option<oneshot::Sender<()>>,
    response: oneshot::Receiver<Result<ObserverPauseProof>>,
}

enum ObserverAudioProofCommand {
    SpeakingStarted {
        respond_to: oneshot::Sender<Result<()>>,
    },
    Pause {
        armed: oneshot::Sender<()>,
        begin: oneshot::Receiver<()>,
        respond_to: oneshot::Sender<Result<ObserverPauseProof>>,
    },
    Resume {
        armed: oneshot::Sender<()>,
        respond_to: oneshot::Sender<Result<ObserverResumeProof>>,
    },
}

async fn await_observer_dave_ready<T, E, F>(ready: F) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: Into<anyhow::Error>,
{
    ready
        .await
        .map_err(Into::into)
        .context("await observer dave readiness")
}

fn post_play_live_contract_state(initial: &LiveContractState) -> LiveContractState {
    let mut state = initial.clone();
    state.saw_track_resolving = false;
    state.saw_buffering = false;
    state.saw_playing = false;
    state.saw_paused = false;
    state.saw_resumed_playing = false;
    state.saw_track_ended = false;
    state
}

fn update_live_contract_snapshot(
    snapshot: &Arc<Mutex<LiveContractState>>,
    state: &LiveContractState,
) {
    *snapshot.lock().unwrap() = state.clone();
}

async fn expected_playback_duration_ms(config: &StagingConfig) -> Result<u64> {
    let mut client = YtMusicClient::connect(config.discord_voice_service_ytmusic_addr.clone())
        .await
        .context("connect ytmusic-service to resolve expected playback duration")?;
    let source = client
        .resolve_playback_source(&config.test_video_id)
        .await
        .with_context(|| {
            format!(
                "resolve expected playback duration for TEST_VIDEO_ID={}",
                config.test_video_id
            )
        })?;
    let duration_ms = source.approx_duration_ms.ok_or_else(|| {
        anyhow!(
            "ytmusic-service did not return an expected duration for TEST_VIDEO_ID={}",
            config.test_video_id
        )
    })?;
    validate_live_test_video_duration(duration_ms, &config.test_video_id)?;

    info!(
        selected_itag = source.selected_itag,
        expected_duration_ms = duration_ms,
        "resolved expected playback duration for live validation",
    );

    Ok(duration_ms)
}

fn validate_live_test_video_duration(duration_ms: u64, video_id: &str) -> Result<()> {
    if duration_ms == 0 {
        bail!("ytmusic-service returned zero expected duration for TEST_VIDEO_ID={video_id}");
    }
    if duration_ms < MIN_LIVE_TEST_VIDEO_DURATION_MS {
        bail!(
            "TEST_VIDEO_ID={video_id} is too short for live staging: expected_duration_ms={duration_ms} but at least {MIN_LIVE_TEST_VIDEO_DURATION_MS}ms is required; choose a longer TEST_VIDEO_ID",
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct GatewayJoinTarget<'a> {
    label: &'a str,
    guild_id: Id<GuildMarker>,
    channel_id: Id<ChannelMarker>,
    user_id: Id<UserMarker>,
    self_mute: bool,
    self_deaf: bool,
}

struct GatewayCleanupTarget<'a> {
    http: &'a HttpClient,
    sender: &'a twilight_gateway::MessageSender,
    shard: &'a mut Shard,
    guild_id: Id<GuildMarker>,
    user_id: Id<UserMarker>,
}

pub async fn run(config: StagingConfig) -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let guild_id = config.guild_id()?;
    let channel_id = config.channel_id()?;

    info!(
        application_id = %config.application_id,
        guild_id = %config.test_guild_id,
        channel_id = %config.test_voice_channel_id,
        service_uri = %config.discord_voice_service_uri,
        ytmusic_addr = %config.discord_voice_service_ytmusic_addr,
        "starting staging live controller",
    );

    let service_http = HttpClient::new(config.bot_token.clone());
    let service_user = service_http
        .current_user()
        .await
        .context("fetch service Discord user")?
        .model()
        .await
        .context("decode service Discord user response")?;

    let observer_http = HttpClient::new(config.observer_bot_token.clone());
    let observer_user = observer_http
        .current_user()
        .await
        .context("fetch observer Discord user")?
        .model()
        .await
        .context("decode observer Discord user response")?;

    let mut service_shard = Shard::new(
        ShardId::ONE,
        config.bot_token.clone(),
        Intents::GUILD_VOICE_STATES,
    );
    let service_sender = service_shard.sender();
    let mut observer_shard = Shard::new(
        ShardId::ONE,
        config.observer_bot_token.clone(),
        Intents::GUILD_VOICE_STATES,
    );
    let observer_sender = observer_shard.sender();

    let service_voice = match join_gateway_voice(
        &service_sender,
        &mut service_shard,
        GatewayJoinTarget {
            label: "service",
            guild_id,
            channel_id,
            user_id: service_user.id,
            self_mute: false,
            self_deaf: false,
        },
    )
    .await
    {
        Ok(context) => context,
        Err(error) => {
            let cleanup = cleanup_gateway_voice(
                &service_http,
                &service_sender,
                &mut service_shard,
                guild_id,
                service_user.id,
            )
            .await;
            return finish_with_failure(&config, Err(error), cleanup, None);
        }
    };

    let flow = run_service_flow(
        &config,
        service_voice,
        &mut service_shard,
        service_user.id,
        &observer_sender,
        &mut observer_shard,
        GatewayJoinTarget {
            label: "observer",
            guild_id,
            channel_id,
            user_id: observer_user.id,
            self_mute: false,
            self_deaf: false,
        },
    )
    .await;

    let cleanup = cleanup_after_flow(
        &config.discord_voice_service_uri,
        flow.service_joined,
        GatewayCleanupTarget {
            http: &service_http,
            sender: &service_sender,
            shard: &mut service_shard,
            guild_id,
            user_id: service_user.id,
        },
        GatewayCleanupTarget {
            http: &observer_http,
            sender: &observer_sender,
            shard: &mut observer_shard,
            guild_id,
            user_id: observer_user.id,
        },
    )
    .await;

    match flow.result {
        Ok(validated) => match finalize_success_evidence(
            Ok(validated),
            cleanup,
            |validated| build_success_evidence(&config, &validated),
            emit_validation_evidence,
        ) {
            Ok(()) => Ok(()),
            Err(error) => {
                emit_failure_evidence(&config, &error, Some(&flow.failure_snapshot));
                Err(error)
            }
        },
        Err(error) => {
            finish_with_failure(&config, Err(error), cleanup, Some(&flow.failure_snapshot))
        }
    }
}

async fn run_service_flow(
    config: &StagingConfig,
    forwarded_voice: ServiceVoiceContext,
    service_shard: &mut Shard,
    service_user_id: Id<UserMarker>,
    observer_sender: &twilight_gateway::MessageSender,
    observer_shard: &mut Shard,
    observer_join: GatewayJoinTarget<'_>,
) -> ServiceFlowOutcome {
    let mut failure_snapshot = FailureEvidenceSnapshot::default();
    let control_client = VoiceServiceClient::connect(config.discord_voice_service_uri.clone())
        .await
        .context("connect discord-voice-service Twilight control client");
    let mut control_client = match control_client {
        Ok(client) => client,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: false,
            };
        }
    };
    let event_client = VoiceServiceClient::connect(config.discord_voice_service_uri.clone())
        .await
        .context("connect discord-voice-service Twilight event client");
    let mut event_client = match event_client {
        Ok(client) => client,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: false,
            };
        }
    };
    let play_client = VoiceServiceClient::connect(config.discord_voice_service_uri.clone())
        .await
        .context("connect discord-voice-service Twilight play client");
    let mut play_client = match play_client {
        Ok(client) => client,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: false,
            };
        }
    };
    let playback_control_client =
        VoiceServiceClient::connect(config.discord_voice_service_uri.clone())
            .await
            .context("connect discord-voice-service Twilight playback control client");
    let mut playback_control_client = match playback_control_client {
        Ok(client) => client,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: false,
            };
        }
    };

    let events = event_client
        .events()
        .await
        .context("subscribe to service events through Twilight adapter");
    let mut events = match events {
        Ok(events) => events,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: false,
            };
        }
    };
    let mut live_contract = LiveContractState::default();
    live_contract.mark_subscribe_events();
    failure_snapshot.live_contract = Some(live_contract.clone());

    let join_result = control_client
        .join_voice(&forwarded_voice)
        .await
        .context("forward authentic voice context to JoinVoice through Twilight adapter");
    if let Err(error) = join_result {
        return ServiceFlowOutcome {
            result: Err(error),
            failure_snapshot,
            service_joined: false,
        };
    }
    live_contract.mark_join_voice();
    failure_snapshot.live_contract = Some(live_contract.clone());

    let initial_live_contract = match timeout(
        LIVE_CONTRACT_TIMEOUT,
        wait_for_initial_voice_ready(&mut events, &config.test_video_id, live_contract),
    )
    .await
    {
        Ok(Ok(state)) => state,
        Ok(Err(error)) => {
            return ServiceFlowOutcome {
                result: Err(error.context("wait for service VoiceReady before Play")),
                failure_snapshot,
                service_joined: true,
            };
        }
        Err(_) => {
            return ServiceFlowOutcome {
                result: Err(anyhow!(
                    "timed out waiting for service VoiceReady before Play after {} seconds",
                    LIVE_CONTRACT_TIMEOUT.as_secs()
                )),
                failure_snapshot,
                service_joined: true,
            };
        }
    };
    let mut initial_live_contract = initial_live_contract;
    failure_snapshot.live_contract = Some(initial_live_contract.clone());

    let update_result = control_client
        .update_voice_context(&forwarded_voice)
        .await
        .context("forward refreshed service voice context through UpdateVoiceContext");
    if let Err(error) = update_result {
        failure_snapshot.live_contract = Some(initial_live_contract.clone());
        return ServiceFlowOutcome {
            result: Err(error),
            failure_snapshot,
            service_joined: true,
        };
    }
    initial_live_contract.mark_update_voice_context();
    failure_snapshot.live_contract = Some(initial_live_contract.clone());

    let initial_live_contract = match timeout(
        LIVE_CONTRACT_TIMEOUT,
        wait_for_initial_voice_ready(&mut events, &config.test_video_id, initial_live_contract),
    )
    .await
    {
        Ok(Ok(state)) => state,
        Ok(Err(error)) => {
            return ServiceFlowOutcome {
                result: Err(error.context("wait for service VoiceReady after UpdateVoiceContext")),
                failure_snapshot,
                service_joined: true,
            };
        }
        Err(_) => {
            return ServiceFlowOutcome {
                result: Err(anyhow!(
                    "timed out waiting for service VoiceReady after UpdateVoiceContext after {} seconds",
                    LIVE_CONTRACT_TIMEOUT.as_secs()
                )),
                failure_snapshot,
                service_joined: true,
            };
        }
    };
    let mut initial_live_contract = initial_live_contract;
    let pre_play_contract_snapshot = Arc::new(Mutex::new(initial_live_contract.clone()));
    if let Err(error) = validate_ready_state_rpc(
        &mut control_client,
        &mut initial_live_contract,
        &pre_play_contract_snapshot,
    )
    .await
    {
        failure_snapshot.live_contract = Some(pre_play_contract_snapshot.lock().unwrap().clone());
        return ServiceFlowOutcome {
            result: Err(error),
            failure_snapshot,
            service_joined: true,
        };
    }
    failure_snapshot.live_contract = Some(initial_live_contract.clone());

    let post_play_live_contract = post_play_live_contract_state(&initial_live_contract);
    let live_contract_snapshot = Arc::new(Mutex::new(post_play_live_contract.clone()));
    failure_snapshot.live_contract = Some(post_play_live_contract.clone());

    let expected_duration_ms =
        match timeout(LIVE_CONTRACT_TIMEOUT, expected_playback_duration_ms(config)).await {
            Ok(Ok(duration_ms)) => duration_ms,
            Ok(Err(error)) => {
                return ServiceFlowOutcome {
                    result: Err(error),
                    failure_snapshot,
                    service_joined: true,
                };
            }
            Err(_) => {
                return ServiceFlowOutcome {
                    result: Err(anyhow!(
                        "timed out resolving expected playback duration after {} seconds",
                        LIVE_CONTRACT_TIMEOUT.as_secs()
                    )),
                    failure_snapshot,
                    service_joined: true,
                };
            }
        };

    info!("joining observer voice session before Play");
    let observer_voice = match join_gateway_voice(observer_sender, observer_shard, observer_join)
        .await
        .context("join observer gateway voice before Play")
    {
        Ok(context) => context,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: true,
            };
        }
    };
    info!("observer gateway voice joined; connecting observer voice session");
    let mut pending_observer =
        match PendingObservedVoiceSession::connect(to_observer_voice_context(&observer_voice))
            .await
            .context("connect pending observer voice session")
        {
            Ok(pending) => pending,
            Err(error) => {
                return ServiceFlowOutcome {
                    result: Err(error),
                    failure_snapshot,
                    service_joined: true,
                };
            }
        };
    pending_observer.set_dave_proposal_authoring(false);
    info!("observer voice session connected; awaiting DAVE readiness during Play");

    let observer_audio_snapshot = Arc::new(Mutex::new(None::<AudioValidationStats>));
    let observer_playback_snapshot = Arc::new(Mutex::new(None::<ObserverPlaybackProof>));
    let play_rpc = async {
        play_client
            .play(config.test_video_id.clone())
            .await
            .context("call Play through Twilight adapter")
    };

    let (track_ended_tx, track_ended_rx) = oneshot::channel();
    let (observer_audio_started_tx, observer_audio_started_rx) = oneshot::channel();
    let (observer_proof_tx, observer_proof_rx) = mpsc::channel(2);
    let service_contract = {
        let live_contract_snapshot = Arc::clone(&live_contract_snapshot);
        let observer_playback_snapshot = Arc::clone(&observer_playback_snapshot);
        async {
            let result = match timeout(
                LIVE_CONTRACT_TIMEOUT,
                wait_for_play_completed_contract_with_controls(
                    &mut events,
                    &config.test_video_id,
                    post_play_live_contract,
                    live_contract_snapshot,
                    &mut playback_control_client,
                    observer_audio_started_rx,
                    observer_proof_tx,
                    observer_playback_snapshot,
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(anyhow!(
                    "play-completion contract timed out after {} seconds",
                    LIVE_CONTRACT_TIMEOUT.as_secs()
                )),
            };
            let state = result?;
            let _ = track_ended_tx.send(());
            Ok(state)
        }
    };
    let observer_audio_snapshot_for_proof = Arc::clone(&observer_audio_snapshot);
    let observer_playback_snapshot_for_outcome = Arc::clone(&observer_playback_snapshot);
    let service_audio_user_id = service_user_id.to_string();
    let combined_validation = async move {
        let mut observer_session =
            await_observer_dave_ready(pending_observer.await_dave_ready(LIVE_CONTRACT_TIMEOUT))
                .await?;
        observer_session.set_dave_proposal_authoring(false);
        let observer_audio = observe_audio_until_track_ended(
            &mut observer_session,
            &service_audio_user_id,
            expected_duration_ms,
            observer_audio_snapshot_for_proof,
            observer_audio_started_tx,
            observer_proof_rx,
            track_ended_rx,
        );
        let ((live_contract, observer_playback), audio_stats) =
            tokio::try_join!(service_contract, observer_audio)?;
        *observer_playback_snapshot_for_outcome.lock().unwrap() = Some(observer_playback.clone());
        Ok(ValidatedLiveOutcome {
            live_contract,
            audio_stats,
            observer_playback,
            expected_duration_ms,
            playback_metrics: None,
            reconnect_probe_metrics: None,
        })
    };

    let validation_and_play = wait_for_play_and_live_contract(combined_validation, play_rpc);
    let service_voice_for_reconnect_probe = forwarded_voice.clone();
    let live_gateway_config = LiveGatewayDriverConfig {
        service_addr: config.discord_voice_service_uri.clone(),
        initial_service_voice: forwarded_voice,
        guild_id: observer_join.guild_id,
        observer_user_id: observer_join.user_id,
    };
    let live_gateway_driver =
        drive_live_gateway_shards(service_shard, observer_shard, live_gateway_config);
    let mut result = tokio::select! {
        result = validation_and_play => result,
        result = live_gateway_driver => {
            match result {
                Ok(()) => Err(anyhow!("live gateway driver ended before playback completed")),
                Err(error) => Err(error),
            }
        }
    };
    failure_snapshot.live_contract = Some(live_contract_snapshot.lock().unwrap().clone());
    failure_snapshot.audio_stats = observer_audio_snapshot.lock().unwrap().clone();
    failure_snapshot.observer_playback = observer_playback_snapshot.lock().unwrap().clone();

    let mut service_joined = true;
    if result.is_err()
        && failure_snapshot.playback_metrics.is_none()
        && let Ok(metrics) = control_client.playback_metrics().await
        && metrics.available
        && metrics.video_id.as_deref() == Some(config.test_video_id.as_str())
    {
        failure_snapshot.playback_metrics = Some((&metrics).into());
    }

    if let Ok(validated) = &mut result {
        validated.live_contract.mark_play();
        update_live_contract_snapshot(&live_contract_snapshot, &validated.live_contract);

        match validate_post_play_control_rpcs(
            &mut control_client,
            &config.discord_voice_service_uri,
            &mut events,
            &service_voice_for_reconnect_probe,
            PostPlayProbeConfig {
                expected_video_id: &config.test_video_id,
            },
            &mut validated.live_contract,
            &live_contract_snapshot,
        )
        .await
        {
            Ok(post_play) => {
                validated.playback_metrics = Some(post_play.playback_metrics);
                validated.reconnect_probe_metrics = Some(post_play.reconnect_probe_metrics);
                failure_snapshot.playback_metrics = validated.playback_metrics.clone();
                failure_snapshot.reconnect_probe_metrics =
                    validated.reconnect_probe_metrics.clone();
                service_joined = false;
            }
            Err(failure) => {
                let PostPlayControlFailure { error, evidence } = *failure;
                failure_snapshot.record_post_play_evidence(evidence);
                result = Err(error);
            }
        }
        failure_snapshot.live_contract = Some(live_contract_snapshot.lock().unwrap().clone());
        failure_snapshot.observer_playback = observer_playback_snapshot.lock().unwrap().clone();
    }

    ServiceFlowOutcome {
        result,
        failure_snapshot,
        service_joined,
    }
}

pub async fn wait_for_play_and_live_contract<State, ContractFuture, PlayFuture>(
    contract_future: ContractFuture,
    play_future: PlayFuture,
) -> Result<State>
where
    ContractFuture: Future<Output = Result<State>>,
    PlayFuture: Future<Output = Result<()>>,
{
    let (state, ()) = tokio::try_join!(contract_future, play_future)?;
    Ok(state)
}

async fn drive_live_gateway_shards(
    service_shard: &mut Shard,
    observer_shard: &mut Shard,
    config: LiveGatewayDriverConfig,
) -> Result<()> {
    let mut update_client = VoiceServiceClient::connect(config.service_addr)
        .await
        .context("connect Twilight adapter client for live gateway driver")?;
    let mut service_voice_tracker = VoiceContextTracker::from_context(config.initial_service_voice);

    loop {
        tokio::select! {
            item = service_shard.next_event(EventTypeFlags::all()) => {
                let Some(event) = next_live_gateway_event("service", item)? else {
                    continue;
                };
                if let Event::VoiceStateUpdate(update) = &event
                    && update.user_id == service_voice_tracker.user_id()
                    && update.guild_id == Some(service_voice_tracker.guild_id())
                {
                    match update.channel_id {
                        None => {
                            bail!("service gateway voice state left the target channel during playback");
                        }
                        Some(channel_id) if channel_id != service_voice_tracker.channel_id() => {
                            bail!(
                                "service gateway voice state moved channels during playback: expected={} actual={}",
                                service_voice_tracker.channel_id(),
                                channel_id
                            );
                        }
                        Some(_) => {}
                    }
                }
                if let Some(context) = service_voice_tracker.observe(&event) {
                    info!("forwarding refreshed service voice context during live playback");
                    update_client
                        .update_voice_context(&context)
                        .await
                        .context("forward refreshed service voice context through Twilight adapter")?;
                }
            }
            item = observer_shard.next_event(EventTypeFlags::all()) => {
                let Some(event) = next_live_gateway_event("observer", item)? else {
                    continue;
                };
                if matches!(
                    event,
                    Event::VoiceStateUpdate(ref update)
                        if update.user_id == config.observer_user_id
                            && update.guild_id == Some(config.guild_id)
                            && update.channel_id.is_none()
                ) {
                    bail!("observer gateway voice state left the target channel during playback");
                }
            }
        }
    }
}

fn next_live_gateway_event(
    label: &str,
    item: Option<std::result::Result<Event, twilight_gateway::error::ReceiveMessageError>>,
) -> Result<Option<Event>> {
    let Some(item) = item else {
        bail!("gateway shard ended while driving {label} live playback events");
    };

    match item {
        Ok(event) => Ok(Some(event)),
        Err(source) => {
            if is_fatal_gateway_receive_error(&source) {
                return Err(anyhow!(
                    "fatal gateway receive error while driving {label} live playback events: {source}"
                ));
            }

            warn!(error = %source, label, "transient gateway receive error during live playback");
            Ok(None)
        }
    }
}

async fn join_gateway_voice(
    sender: &twilight_gateway::MessageSender,
    shard: &mut Shard,
    target: GatewayJoinTarget<'_>,
) -> Result<ServiceVoiceContext> {
    sender
        .command(&join_voice_channel(
            target.guild_id,
            target.channel_id,
            target.self_deaf,
            target.self_mute,
        ))
        .with_context(|| format!("send {} gateway voice join command", target.label))?;

    wait_for_authentic_voice_context(
        target.label,
        shard,
        target.guild_id,
        target.channel_id,
        target.user_id,
    )
    .await
}

async fn wait_for_authentic_voice_context(
    label: &str,
    shard: &mut Shard,
    guild_id: Id<GuildMarker>,
    channel_id: Id<ChannelMarker>,
    user_id: Id<UserMarker>,
) -> Result<ServiceVoiceContext> {
    let mut tracker = VoiceContextTracker::new(guild_id, channel_id, user_id);

    let deadline = Instant::now() + AUTHENTIC_VOICE_EVENT_TIMEOUT;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                anyhow!("timed out waiting for {label} authentic voice gateway events")
            })?;

        let next = timeout(remaining, shard.next_event(EventTypeFlags::all()))
            .await
            .map_err(|_| anyhow!("timed out waiting for {label} authentic voice gateway events"))?;

        let Some(item) = next else {
            bail!("gateway shard ended before {label} voice events were observed");
        };

        let event = match item {
            Ok(event) => event,
            Err(source) => {
                if is_fatal_gateway_receive_error(&source) {
                    return Err(anyhow!(
                        "fatal gateway receive error while waiting for {label} voice events: {source}"
                    ));
                }

                warn!(error = %source, label, "transient gateway receive error");
                continue;
            }
        };

        if let Some(context) = tracker.observe(&event) {
            return Ok(context);
        }
    }
}

async fn observe_audio_until_track_ended(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
    expected_duration_ms: u64,
    snapshot: Arc<Mutex<Option<AudioValidationStats>>>,
    audio_started: oneshot::Sender<()>,
    mut proof_commands: mpsc::Receiver<ObserverAudioProofCommand>,
    mut track_ended: oneshot::Receiver<()>,
) -> Result<AudioValidationStats> {
    let deadline = Instant::now() + LIVE_CONTRACT_TIMEOUT;
    let mut accumulator = AudioValidationAccumulator::new();
    let mut last_stats: Option<AudioValidationStats> = None;
    let mut audio_started = Some(audio_started);
    let mut proof_commands_closed = false;
    let mut explicit_pause_boundary_observed = false;

    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(observer_threshold_error(
                last_stats.as_ref(),
                expected_duration_ms,
            ));
        };

        tokio::select! {
            track_ended_result = &mut track_ended => {
                track_ended_result
                    .context("service play-completion contract ended before TrackEnded notification")?;
                let stats = drain_observer_audio_after_track_ended(
                    observer_session,
                    expected_user_id,
                    &mut accumulator,
                    &snapshot,
                    &mut audio_started,
                )
                .await
                .context("observer audio drain after TrackEnded failed")?;
                *snapshot.lock().unwrap() = Some(stats.clone());
                if observer_thresholds_satisfied(&stats, expected_duration_ms) {
                    return Ok(stats);
                }

                return Err(observer_threshold_error(Some(&stats), expected_duration_ms));
            }
            command = proof_commands.recv(), if !proof_commands_closed => {
                match command {
                    Some(ObserverAudioProofCommand::SpeakingStarted { respond_to }) => {
                        let result = prove_observer_speaking_started(
                            observer_session,
                            expected_user_id,
                        )
                        .await;
                        if result.is_ok() {
                            info!("observer proved service Speaking 1 before Pause");
                        }
                        let _ = respond_to.send(result);
                    }
                    Some(ObserverAudioProofCommand::Pause {
                        armed,
                        begin,
                        respond_to,
                    }) => {
                        let _ = armed.send(());
                        match wait_for_observer_pause_proof_begin(
                            observer_session,
                            expected_user_id,
                            &mut accumulator,
                            &snapshot,
                            &mut audio_started,
                            begin,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(error) => {
                                explicit_pause_boundary_observed = false;
                                let _ = respond_to.send(Err(error));
                                continue;
                            }
                        }
                        accumulator.start_controlled_pause();
                        let result = prove_observer_pause_silence(
                            observer_session,
                            expected_user_id,
                            &mut accumulator,
                            &snapshot,
                            &mut audio_started,
                        )
                        .await;
                        match &result {
                            Ok(proof) => {
                                info!(
                                    silence_ms = proof.silence_ms,
                                    "observer proved Pause by observing service stop boundary and no service audio",
                                );
                                explicit_pause_boundary_observed = proof.rtp_silence_observed;
                                accumulator.reset_wall_clock_baseline_after_controlled_pause();
                            }
                            Err(_) => {
                                explicit_pause_boundary_observed = false;
                            }
                        }
                        let _ = respond_to.send(result);
                        last_stats = Some(accumulator.stats());
                    }
                    Some(ObserverAudioProofCommand::Resume { armed, respond_to }) => {
                        observer_session.set_dave_proposal_authoring(true);
                        let _ = armed.send(());
                        let result = prove_observer_resume_audio(
                            observer_session,
                            expected_user_id,
                            &mut accumulator,
                            &snapshot,
                            &mut audio_started,
                            explicit_pause_boundary_observed,
                        )
                        .await;
                        observer_session.set_dave_proposal_authoring(false);
                        if let Ok(proof) = &result {
                            if proof.speaking_started {
                                info!(
                                    observed_packet_count = proof.observed_packet_count,
                                    "observer proved Resume by observing service speaking start and receiving service audio",
                                );
                            } else {
                                info!(
                                    observed_packet_count = proof.observed_packet_count,
                                    "observer proved Resume by receiving service audio after explicit Pause boundary without gateway Speaking 1 echo",
                                );
                            }
                        }
                        let _ = respond_to.send(result);
                        last_stats = Some(accumulator.stats());
                    }
                    None => {
                        proof_commands_closed = true;
                    }
                }
            }
            frame = observer_session.receive_audio_frame_from(expected_user_id, remaining) => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        let context = match last_stats.as_ref() {
                            Some(stats) => format!(
                                "observer audio proof failed after observed_packet_count={} decoded_audio_ms={} non_silent_audio_ms={} expected_duration_ms={} required_decoded_audio_ms={}",
                                stats.observed_packet_count,
                                stats.decoded_audio_ms,
                                stats.non_silent_audio_ms,
                                expected_duration_ms,
                                required_observer_decoded_audio_ms(expected_duration_ms),
                            ),
                            None => {
                                "observer audio proof failed before any packets were validated".to_owned()
                            }
                        };
                        return Err(error).context(context);
                    }
                };

                let stats = record_observer_audio_frame(
                    frame,
                    &mut accumulator,
                    &snapshot,
                    &mut audio_started,
                )?;
                last_stats = Some(stats);
            }
        }
    }
}

async fn drain_observer_audio_after_track_ended(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
    accumulator: &mut AudioValidationAccumulator,
    snapshot: &Arc<Mutex<Option<AudioValidationStats>>>,
    audio_started: &mut Option<oneshot::Sender<()>>,
) -> Result<AudioValidationStats> {
    accumulator.start_natural_end_drain();
    let deadline = Instant::now() + OBSERVER_TRACK_ENDED_DRAIN_GRACE;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return observer_accumulator_stats(accumulator);
        };

        match observer_session
            .receive_audio_frame_from(expected_user_id, remaining)
            .await
        {
            Ok(frame) => {
                record_observer_audio_frame(frame, accumulator, snapshot, audio_started)?;
            }
            Err(error) if is_voice_receive_timeout(&error) => {
                return observer_accumulator_stats(accumulator);
            }
            Err(error) => return Err(error).context("observer drain receive failed"),
        }
    }
}

fn observer_accumulator_stats(
    accumulator: &AudioValidationAccumulator,
) -> Result<AudioValidationStats> {
    let stats = accumulator.stats();
    if stats.observed_packet_count == 0 {
        bail!("observer audio proof completed without validated packets");
    }
    Ok(stats)
}

async fn wait_for_observer_pause_proof_begin(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
    accumulator: &mut AudioValidationAccumulator,
    snapshot: &Arc<Mutex<Option<AudioValidationStats>>>,
    audio_started: &mut Option<oneshot::Sender<()>>,
    mut begin: oneshot::Receiver<()>,
) -> Result<Option<AudioValidationStats>> {
    let mut last_stats = None;

    loop {
        match begin.try_recv() {
            Ok(()) => return Ok(last_stats),
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(oneshot::error::TryRecvError::Closed) => {
                bail!("observer Pause proof begin signal was dropped before Pause started");
            }
        }

        match observer_session
            .receive_activity_from(expected_user_id, PAUSE_PROOF_ARM_POLL_INTERVAL)
            .await
        {
            Ok(ObservedVoiceActivity::Audio(frame)) => {
                last_stats = Some(record_observer_audio_frame(
                    frame,
                    accumulator,
                    snapshot,
                    audio_started,
                )?);
            }
            Ok(ObservedVoiceActivity::RtpPacket(packet)) => {
                info!(
                    sequence = packet.sequence,
                    ssrc = packet.ssrc,
                    "observer saw pre-Pause RTP packet while Pause proof was armed",
                );
            }
            Ok(ObservedVoiceActivity::Speaking(state)) => {
                info!(
                    user_id = %state.user_id,
                    ssrc = state.ssrc,
                    speaking = state.speaking,
                    "observer saw pre-Pause speaking state while Pause proof was armed",
                );
            }
            Ok(ObservedVoiceActivity::Disconnect(user_id)) => {
                bail!(
                    "observer saw service voice client disconnect while Pause proof was armed before Pause started (user_id={user_id})"
                );
            }
            Err(error) if is_voice_receive_timeout(&error) => {}
            Err(error) => {
                return Err(error).context("observer pause proof failed while armed before Pause");
            }
        }
    }
}

async fn prove_observer_pause_silence(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
    accumulator: &mut AudioValidationAccumulator,
    snapshot: &Arc<Mutex<Option<AudioValidationStats>>>,
    audio_started: &mut Option<oneshot::Sender<()>>,
) -> Result<ObserverPauseProof> {
    let proof_started = Instant::now();
    let mut gateway_speaking_stopped = false;
    let mut rtp_silence_observed = false;
    let mut silence_deadline = None;
    let mut last_service_activity_at = proof_started;
    let mut consecutive_stop_silence_frames = 0usize;
    let transition_deadline = Instant::now() + OBSERVER_SPEAKING_STATE_TIMEOUT;

    loop {
        let mut active_deadline = silence_deadline.unwrap_or(transition_deadline);
        if !gateway_speaking_stopped
            && !rtp_silence_observed
            && accumulator.stats().observed_packet_count > 0
        {
            let inferred_silence_deadline =
                last_service_activity_at + PAUSE_OBSERVER_SILENCE_DURATION;
            if inferred_silence_deadline < active_deadline {
                active_deadline = inferred_silence_deadline;
            }
        }
        let Some(remaining) = active_deadline.checked_duration_since(Instant::now()) else {
            if rtp_silence_observed {
                return Ok(ObserverPauseProof {
                    silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                    gateway_speaking_stopped,
                    rtp_silence_observed: true,
                });
            }
            if accumulator.stats().observed_packet_count > 0
                && last_service_activity_at.elapsed() >= PAUSE_OBSERVER_SILENCE_DURATION
            {
                info!(
                    silence_ms = duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                    "observer observed Pause RTP silence without explicit service stop-silence boundary"
                );
                return Ok(ObserverPauseProof {
                    silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                    gateway_speaking_stopped,
                    rtp_silence_observed: false,
                });
            }

            if gateway_speaking_stopped {
                return Ok(ObserverPauseProof {
                    silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                    gateway_speaking_stopped,
                    rtp_silence_observed: false,
                });
            }

            bail!(
                "observer did not receive service speaking disappearance within {} seconds",
                OBSERVER_SPEAKING_STATE_TIMEOUT.as_secs(),
            );
        };

        match observer_session
            .receive_activity_from(expected_user_id, remaining)
            .await
        {
            Ok(ObservedVoiceActivity::Audio(frame)) => {
                let sequence = frame.sequence;
                let is_stop_silence = frame.payload.as_ref() == OPUS_STOP_SILENCE_FRAME.as_slice();
                let stats =
                    record_observer_audio_frame(frame, accumulator, snapshot, audio_started)?;
                last_service_activity_at = Instant::now();
                consecutive_stop_silence_frames = if is_stop_silence {
                    consecutive_stop_silence_frames.saturating_add(1)
                } else {
                    0
                };
                if !rtp_silence_observed {
                    if consecutive_stop_silence_frames >= PAUSE_STOP_SILENCE_FRAME_COUNT {
                        info!(
                            silence_frames = consecutive_stop_silence_frames,
                            "observer saw service stop-silence tail during Pause proof"
                        );
                        rtp_silence_observed = true;
                        silence_deadline = Some(Instant::now() + PAUSE_OBSERVER_SILENCE_DURATION);
                    }
                    continue;
                }
                let proof_elapsed_ms = Instant::now()
                    .saturating_duration_since(proof_started)
                    .as_millis();
                bail!(
                    "observer received service audio during Pause proof after speaking disappearance (sequence={} observed_packet_count={} decoded_audio_ms={} proof_elapsed_ms={})",
                    sequence,
                    stats.observed_packet_count,
                    stats.decoded_audio_ms,
                    proof_elapsed_ms,
                );
            }
            Ok(ObservedVoiceActivity::RtpPacket(packet)) => {
                last_service_activity_at = Instant::now();
                consecutive_stop_silence_frames = 0;
                if !rtp_silence_observed {
                    continue;
                }
                let proof_elapsed_ms = Instant::now()
                    .saturating_duration_since(proof_started)
                    .as_millis();
                bail!(
                    "observer received service voice RTP packet during Pause proof after speaking disappearance (sequence={} ssrc={} proof_elapsed_ms={})",
                    packet.sequence,
                    packet.ssrc,
                    proof_elapsed_ms,
                );
            }
            Ok(ObservedVoiceActivity::Speaking(state)) => {
                last_service_activity_at = Instant::now();
                if state.speaking & SPEAKING_FLAG_MICROPHONE != 0 {
                    consecutive_stop_silence_frames = 0;
                    if gateway_speaking_stopped {
                        bail!(
                            "observer saw service microphone Speaking {} after Pause speaking disappearance",
                            state.speaking
                        );
                    }
                    info!(
                        user_id = %state.user_id,
                        ssrc = state.ssrc,
                        speaking = state.speaking,
                        "observer saw pre-Pause microphone service speaking state"
                    );
                    continue;
                }
                info!(
                    user_id = %state.user_id,
                    ssrc = state.ssrc,
                    speaking = state.speaking,
                    "observer saw service speaking disappear during Pause proof"
                );
                gateway_speaking_stopped = true;
                silence_deadline = Some(Instant::now() + PAUSE_OBSERVER_SILENCE_DURATION);
            }
            Ok(ObservedVoiceActivity::Disconnect(user_id)) => {
                bail!(
                    "observer saw service voice client disconnect during Pause proof (user_id={user_id})"
                );
            }
            Err(error) if is_voice_receive_timeout(&error) => {
                if rtp_silence_observed {
                    return Ok(ObserverPauseProof {
                        silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                        gateway_speaking_stopped,
                        rtp_silence_observed: true,
                    });
                }
                if accumulator.stats().observed_packet_count > 0
                    && last_service_activity_at.elapsed() >= PAUSE_OBSERVER_SILENCE_DURATION
                {
                    info!(
                        silence_ms = duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                        "observer observed Pause RTP silence without explicit service stop-silence boundary"
                    );
                    return Ok(ObserverPauseProof {
                        silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                        gateway_speaking_stopped,
                        rtp_silence_observed: false,
                    });
                }
                if gateway_speaking_stopped {
                    return Ok(ObserverPauseProof {
                        silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                        gateway_speaking_stopped,
                        rtp_silence_observed: false,
                    });
                }
                bail!(
                    "observer timed out waiting for service speaking disappearance during Pause proof"
                );
            }
            Err(error) => {
                return Err(error).context("observer pause proof failed during silence window");
            }
        }
    }
}

async fn prove_observer_speaking_started(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
) -> Result<()> {
    observer_session
        .receive_speaking_state_from(expected_user_id, 1, OBSERVER_SPEAKING_STATE_TIMEOUT)
        .await
        .context("observer pre-Pause proof did not observe service Speaking 1")?;
    Ok(())
}

async fn prove_observer_resume_audio(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
    accumulator: &mut AudioValidationAccumulator,
    snapshot: &Arc<Mutex<Option<AudioValidationStats>>>,
    audio_started: &mut Option<oneshot::Sender<()>>,
    explicit_pause_boundary_observed: bool,
) -> Result<ObserverResumeProof> {
    let start_count = accumulator.stats().observed_packet_count;
    let resume_decoded_audio_start_ms = accumulator.stats().decoded_audio_ms;
    let mut speaking_started = false;
    let mut accepted_audio_without_gateway_speaking_logged = false;
    let deadline = Instant::now() + RESUME_OBSERVER_AUDIO_TIMEOUT;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            bail!(
                "observer did not receive resumed service audio within {} seconds (speaking_started={} explicit_pause_boundary_observed={} observed_after_resume={} required={})",
                RESUME_OBSERVER_AUDIO_TIMEOUT.as_secs(),
                speaking_started,
                explicit_pause_boundary_observed,
                accumulator
                    .stats()
                    .observed_packet_count
                    .saturating_sub(start_count),
                RESUME_OBSERVER_PACKET_TARGET,
            );
        };

        match observer_session
            .receive_activity_from(expected_user_id, remaining)
            .await
        {
            Ok(ObservedVoiceActivity::Audio(frame)) => {
                if !speaking_started && !explicit_pause_boundary_observed {
                    bail!("observer received resumed service audio before service Speaking 1");
                }
                if !speaking_started && !accepted_audio_without_gateway_speaking_logged {
                    info!(
                        sequence = frame.sequence,
                        ssrc = frame.ssrc,
                        "observer accepted resumed service audio after explicit Pause boundary without gateway Speaking 1 echo"
                    );
                    accepted_audio_without_gateway_speaking_logged = true;
                }
                let stats =
                    record_observer_audio_frame(frame, accumulator, snapshot, audio_started)?;
                let observed_after_resume = stats.observed_packet_count.saturating_sub(start_count);
                if observed_after_resume >= RESUME_OBSERVER_PACKET_TARGET {
                    return Ok(ObserverResumeProof {
                        observed_packet_count: observed_after_resume,
                        speaking_started,
                        resume_decoded_audio_start_ms,
                    });
                }
            }
            Ok(ObservedVoiceActivity::RtpPacket(packet)) => {
                if !speaking_started && !explicit_pause_boundary_observed {
                    bail!(
                        "observer received resumed service RTP before service Speaking 1 (sequence={} ssrc={})",
                        packet.sequence,
                        packet.ssrc,
                    );
                }
                info!(
                    user_id = %packet.user_id,
                    ssrc = packet.ssrc,
                    sequence = packet.sequence,
                    "observer saw undecoded service RTP packet during Resume proof"
                );
            }
            Ok(ObservedVoiceActivity::Speaking(state)) => {
                if state.speaking & SPEAKING_FLAG_MICROPHONE != 0 {
                    info!(
                        user_id = %state.user_id,
                        ssrc = state.ssrc,
                        speaking = state.speaking,
                        "observer saw microphone service speaking state during Resume proof"
                    );
                    speaking_started = true;
                    let observed_after_resume = accumulator
                        .stats()
                        .observed_packet_count
                        .saturating_sub(start_count);
                    if observed_after_resume >= RESUME_OBSERVER_PACKET_TARGET {
                        return Ok(ObserverResumeProof {
                            observed_packet_count: observed_after_resume,
                            speaking_started: true,
                            resume_decoded_audio_start_ms,
                        });
                    }
                } else {
                    info!(
                        user_id = %state.user_id,
                        ssrc = state.ssrc,
                        speaking = state.speaking,
                        "observer saw non-microphone service speaking state during Resume proof"
                    );
                }
            }
            Ok(ObservedVoiceActivity::Disconnect(user_id)) => {
                bail!("observer saw service disconnect during Resume proof (user_id={user_id})");
            }
            Err(error) if is_voice_receive_timeout(&error) => {
                bail!(
                    "observer timed out waiting for resumed service audio (speaking_started={} explicit_pause_boundary_observed={} observed_after_resume={} required={})",
                    speaking_started,
                    explicit_pause_boundary_observed,
                    accumulator
                        .stats()
                        .observed_packet_count
                        .saturating_sub(start_count),
                    RESUME_OBSERVER_PACKET_TARGET,
                );
            }
            Err(error) => return Err(error).context("observer resume proof failed"),
        }
    }
}

fn record_observer_audio_frame(
    frame: ObservedAudioFrame,
    accumulator: &mut AudioValidationAccumulator,
    snapshot: &Arc<Mutex<Option<AudioValidationStats>>>,
    audio_started: &mut Option<oneshot::Sender<()>>,
) -> Result<AudioValidationStats> {
    let sequence = frame.sequence;
    let timestamp = frame.timestamp;
    let received_at = frame.received_at;
    let payload_len = frame.payload.len();
    let stats = accumulator
        .observe_packet_at(
            ObservedOpusPacket {
                sequence,
                timestamp,
                payload: frame.payload.as_ref(),
            },
            received_at,
        )
        .with_context(|| {
            format!(
                "analyze observer audio packet sequence={sequence} timestamp={timestamp} payload_len={payload_len}"
            )
        })?;
    *snapshot.lock().unwrap() = Some(stats.clone());
    if let Some(audio_started) = audio_started.take() {
        let _ = audio_started.send(());
    }
    Ok(stats)
}

fn is_voice_receive_timeout(error: &VoiceError) -> bool {
    matches!(error, VoiceError::InvalidState("voice receive timed out"))
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn observer_thresholds_satisfied(stats: &AudioValidationStats, expected_duration_ms: u64) -> bool {
    stats.observed_packet_count >= MIN_OBSERVED_PACKET_COUNT
        && stats.decoded_audio_ms >= required_observer_decoded_audio_ms(expected_duration_ms)
        && stats.wall_clock_elapsed_ms > 0
        && stats.decoded_audio_to_wall_clock_ratio_ppm > 0
        && stats.decoded_audio_to_wall_clock_ratio_ppm >= MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM
        && stats.decoded_audio_to_wall_clock_ratio_ppm <= MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM
        && stats.non_silent_audio_ms >= MIN_NON_SILENT_AUDIO_MS
        && stats.rtp_inter_arrival.samples > 0
        && unclassified_observer_anomaly_count(
            stats,
            "rtp_gap_gte_100ms",
            stats.rtp_gap_count_gte_100ms,
        ) == 0
        && stats.rtp_buffering_event_count == 0
        && stats.rtp_speed_change_total_abs_us
            <= observer_speed_change_total_abs_budget_us(expected_duration_ms)
        && stats.decoded_audio_tempo_window_count > 0
        && stats.decoded_audio_tempo_window_fast_count == 0
        && stats.decoded_audio_tempo_window_slow_count == 0
        && stats.decoded_audio_short_tempo_window_count > 0
        && observer_short_tempo_windows_within_jitter_budget(stats)
        && (!observer_post_source_buffer_window_required(expected_duration_ms)
            || stats.decoded_audio_tempo_window_post_source_buffer_count
                >= MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS)
        && stats.rtp_inter_arrival.p95_ms <= RTP_INTERVAL_P95_BUDGET_MS
        && stats.rtp_inter_arrival.p99_ms <= RTP_INTERVAL_P99_BUDGET_MS
        && stats.rtp_inter_arrival.max_ms < RTP_INTERVAL_MAX_BUDGET_MS
}

fn observer_threshold_failure_reason(
    stats: &AudioValidationStats,
    expected_duration_ms: u64,
) -> &'static str {
    if stats.observed_packet_count < MIN_OBSERVED_PACKET_COUNT {
        return "observer_audio_missing_packets";
    }
    if stats.decoded_audio_ms < required_observer_decoded_audio_ms(expected_duration_ms) {
        return "observer_audio_incomplete";
    }
    if stats.wall_clock_elapsed_ms == 0 || stats.decoded_audio_to_wall_clock_ratio_ppm == 0 {
        return "observer_audio_missing_timing";
    }
    if stats.decoded_audio_to_wall_clock_ratio_ppm > MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM {
        return "observer_audio_tempo_fast";
    }
    if stats.decoded_audio_to_wall_clock_ratio_ppm < MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM {
        return "observer_audio_tempo_slow";
    }
    if stats.non_silent_audio_ms < MIN_NON_SILENT_AUDIO_MS {
        return "observer_audio_silent";
    }
    if stats.rtp_inter_arrival.samples == 0 {
        return "observer_audio_missing_rtp_intervals";
    }
    if unclassified_observer_anomaly_count(
        stats,
        "rtp_gap_gte_100ms",
        stats.rtp_gap_count_gte_100ms,
    ) != 0
    {
        return "observer_audio_rtp_gap";
    }
    if stats.rtp_buffering_event_count != 0 {
        return "observer_audio_buffered";
    }
    if stats.rtp_speed_change_total_abs_us
        > observer_speed_change_total_abs_budget_us(expected_duration_ms)
    {
        return "observer_audio_speed_changed";
    }
    if stats.decoded_audio_tempo_window_count == 0 {
        return "observer_audio_missing_tempo_windows";
    }
    if stats.decoded_audio_tempo_window_fast_count != 0 {
        return "observer_audio_tempo_fast";
    }
    if stats.decoded_audio_tempo_window_slow_count != 0 {
        return "observer_audio_tempo_slow";
    }
    if stats.decoded_audio_short_tempo_window_count == 0 {
        return "observer_audio_missing_short_tempo_windows";
    }
    match stats.decoded_audio_short_tempo_window_fastest.as_ref() {
        Some(window) if !observer_tempo_window_within_jitter_budget(window) => {
            return observer_tempo_window_failure_reason(window);
        }
        Some(_) => {}
        None => return "observer_audio_missing_short_tempo_windows",
    }
    match stats.decoded_audio_short_tempo_window_slowest.as_ref() {
        Some(window) if !observer_tempo_window_within_jitter_budget(window) => {
            return observer_tempo_window_failure_reason(window);
        }
        Some(_) => {}
        None => return "observer_audio_missing_short_tempo_windows",
    }
    if observer_post_source_buffer_window_required(expected_duration_ms)
        && stats.decoded_audio_tempo_window_post_source_buffer_count
            < MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS
    {
        return "observer_audio_insufficient_post_source_tempo_windows";
    }
    if stats.rtp_inter_arrival.p95_ms > RTP_INTERVAL_P95_BUDGET_MS
        || stats.rtp_inter_arrival.p99_ms > RTP_INTERVAL_P99_BUDGET_MS
        || stats.rtp_inter_arrival.max_ms >= RTP_INTERVAL_MAX_BUDGET_MS
    {
        return "observer_audio_rtp_jitter";
    }

    "observer_audio_incomplete"
}

fn observer_short_tempo_windows_within_jitter_budget(stats: &AudioValidationStats) -> bool {
    stats
        .decoded_audio_short_tempo_window_fastest
        .as_ref()
        .is_some_and(observer_tempo_window_within_jitter_budget)
        && stats
            .decoded_audio_short_tempo_window_slowest
            .as_ref()
            .is_some_and(observer_tempo_window_within_jitter_budget)
}

fn observer_tempo_window_within_jitter_budget(window: &AudioTempoWindowEvidence) -> bool {
    let (min_ratio_ppm, max_ratio_ppm) = observer_tempo_window_ratio_bounds(window);
    window.ratio_ppm >= min_ratio_ppm && window.ratio_ppm <= max_ratio_ppm
}

fn observer_tempo_window_failure_reason(window: &AudioTempoWindowEvidence) -> &'static str {
    let (min_ratio_ppm, max_ratio_ppm) = observer_tempo_window_ratio_bounds(window);
    if window.ratio_ppm > max_ratio_ppm {
        "observer_audio_short_tempo_fast"
    } else if window.ratio_ppm < min_ratio_ppm {
        "observer_audio_short_tempo_slow"
    } else {
        "observer_audio_short_tempo_inconsistent"
    }
}

fn observer_tempo_window_ratio_bounds(window: &AudioTempoWindowEvidence) -> (u64, u64) {
    if window.window_packet_count >= OBSERVER_STRICT_SHORT_WINDOW_MIN_PACKETS {
        (
            OBSERVER_SHORT_WINDOW_MIN_RATIO_PPM,
            OBSERVER_SHORT_WINDOW_MAX_RATIO_PPM,
        )
    } else {
        (
            OBSERVER_MICRO_WINDOW_MIN_RATIO_PPM,
            OBSERVER_MICRO_WINDOW_MAX_RATIO_PPM,
        )
    }
}

fn observer_speed_change_total_abs_budget_us(expected_duration_ms: u64) -> u64 {
    expected_duration_ms
        .saturating_mul(1_000)
        .checked_div(OBSERVER_SPEED_CHANGE_TOTAL_ABS_RATIO_DENOMINATOR)
        .unwrap_or(OBSERVER_SPEED_CHANGE_TOTAL_ABS_MAX_BUDGET_US)
        .clamp(
            OBSERVER_SPEED_CHANGE_TOTAL_ABS_MIN_BUDGET_US,
            OBSERVER_SPEED_CHANGE_TOTAL_ABS_MAX_BUDGET_US,
        )
}

fn unclassified_observer_anomaly_count(
    stats: &AudioValidationStats,
    kind: &str,
    total_count: u64,
) -> u64 {
    let controlled_count = stats
        .observer_anomalies
        .iter()
        .filter(|anomaly| anomaly.kind == kind && anomaly.classification == "controlled_pause")
        .count() as u64;
    total_count.saturating_sub(controlled_count)
}

fn observer_post_source_buffer_window_required(expected_duration_ms: u64) -> bool {
    expected_duration_ms >= SOURCE_PLAYBACK_BUFFER_TARGET_MS.saturating_add(1_000)
}

fn required_observer_decoded_audio_ms(expected_duration_ms: u64) -> u64 {
    let ratio_floor =
        expected_duration_ms.saturating_mul(OBSERVER_AUDIO_DURATION_MIN_RATIO_PERCENT) / 100;
    let tolerance_floor = expected_duration_ms.saturating_sub(OBSERVER_AUDIO_DURATION_TOLERANCE_MS);
    ratio_floor
        .max(tolerance_floor)
        .max(MIN_DECODED_AUDIO_MS.min(expected_duration_ms))
        .min(expected_duration_ms)
}

fn observer_threshold_error(
    stats: Option<&AudioValidationStats>,
    expected_duration_ms: u64,
) -> anyhow::Error {
    let required_decoded_audio_ms = required_observer_decoded_audio_ms(expected_duration_ms);
    let speed_change_total_abs_budget_us =
        observer_speed_change_total_abs_budget_us(expected_duration_ms);
    match stats {
        Some(stats) => anyhow!(
            "observer audio proof finished before thresholds (threshold_failure_reason={} observed_packet_count={} decoded_audio_ms={} required_decoded_audio_ms={} expected_duration_ms={} observer_wall_clock_elapsed_ms={} decoded_audio_to_wall_clock_ratio_ppm={} min_ratio_ppm={} max_ratio_ppm={} non_silent_audio_ms={} required_non_silent_audio_ms={} rtp_gap_count_gte_100ms={} unclassified_rtp_gap_count_gte_100ms={} rtp_fast_interval_count={} unclassified_rtp_fast_interval_count={} rtp_fast_interval_min_ms={} rtp_fast_interval_min_us={} rtp_buffering_event_count={} rtp_buffering_total_us={} rtp_buffering_max_us={} rtp_speed_change_total_abs_us={} rtp_speed_change_total_abs_budget_us={} rtp_speed_change_total_fast_us={} rtp_speed_change_total_slow_us={} observer_anomalies={:?} decoded_audio_tempo_window_count={} decoded_audio_tempo_window_post_source_buffer_count={} decoded_audio_tempo_window_min_ratio_ppm={} decoded_audio_tempo_window_max_ratio_ppm={} decoded_audio_tempo_window_fast_count={} decoded_audio_tempo_window_fastest_ratio_ppm={} decoded_audio_tempo_window_slow_count={} decoded_audio_tempo_window_slowest_ratio_ppm={} decoded_audio_short_tempo_window_count={} decoded_audio_short_tempo_window_fast_count={} decoded_audio_short_tempo_window_slow_count={} decoded_audio_short_tempo_window_fastest={:?} decoded_audio_short_tempo_window_slowest={:?} rtp_p95_ms={} rtp_p99_ms={} rtp_max_ms={})",
            observer_threshold_failure_reason(stats, expected_duration_ms),
            stats.observed_packet_count,
            stats.decoded_audio_ms,
            required_decoded_audio_ms,
            expected_duration_ms,
            stats.wall_clock_elapsed_ms,
            stats.decoded_audio_to_wall_clock_ratio_ppm,
            MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM,
            MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM,
            stats.non_silent_audio_ms,
            MIN_NON_SILENT_AUDIO_MS,
            stats.rtp_gap_count_gte_100ms,
            unclassified_observer_anomaly_count(
                stats,
                "rtp_gap_gte_100ms",
                stats.rtp_gap_count_gte_100ms,
            ),
            stats.rtp_fast_interval_count,
            unclassified_observer_anomaly_count(
                stats,
                "rtp_fast_interval",
                stats.rtp_fast_interval_count,
            ),
            stats.rtp_fast_interval_min_ms,
            stats.rtp_fast_interval_min_us,
            stats.rtp_buffering_event_count,
            stats.rtp_buffering_total_us,
            stats.rtp_buffering_max_us,
            stats.rtp_speed_change_total_abs_us,
            speed_change_total_abs_budget_us,
            stats.rtp_speed_change_total_fast_us,
            stats.rtp_speed_change_total_slow_us,
            stats.observer_anomalies,
            stats.decoded_audio_tempo_window_count,
            stats.decoded_audio_tempo_window_post_source_buffer_count,
            stats.decoded_audio_tempo_window_min_ratio_ppm,
            stats.decoded_audio_tempo_window_max_ratio_ppm,
            stats.decoded_audio_tempo_window_fast_count,
            stats.decoded_audio_tempo_window_fastest_ratio_ppm,
            stats.decoded_audio_tempo_window_slow_count,
            stats.decoded_audio_tempo_window_slowest_ratio_ppm,
            stats.decoded_audio_short_tempo_window_count,
            stats.decoded_audio_short_tempo_window_fast_count,
            stats.decoded_audio_short_tempo_window_slow_count,
            stats.decoded_audio_short_tempo_window_fastest,
            stats.decoded_audio_short_tempo_window_slowest,
            stats.rtp_inter_arrival.p95_ms,
            stats.rtp_inter_arrival.p99_ms,
            stats.rtp_inter_arrival.max_ms,
        ),
        None => anyhow!(
            "observer audio proof timed out before any packets were received (expected_duration_ms={} required_decoded_audio_ms={})",
            expected_duration_ms,
            required_decoded_audio_ms,
        ),
    }
}

#[cfg(test)]
async fn wait_for_play_completed_contract(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    state: LiveContractState,
) -> Result<LiveContractState> {
    wait_for_play_completed_contract_with_snapshot(events, expected_video_id, state, None).await
}

#[cfg(test)]
async fn wait_for_play_completed_contract_with_snapshot(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    mut state: LiveContractState,
    snapshot: Option<Arc<Mutex<LiveContractState>>>,
) -> Result<LiveContractState> {
    loop {
        let maybe_event = events.next().await;
        let event = next_session_event(maybe_event)?;
        state.observe_event(event, expected_video_id)?;
        if let Some(snapshot) = &snapshot {
            update_live_contract_snapshot(snapshot, &state);
        }
        if state.saw_track_ended {
            return Ok(state);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_play_completed_contract_with_controls(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    mut state: LiveContractState,
    snapshot: Arc<Mutex<LiveContractState>>,
    controls: &mut VoiceServiceClient,
    observer_audio_started: oneshot::Receiver<()>,
    observer_proof_tx: mpsc::Sender<ObserverAudioProofCommand>,
    observer_playback_snapshot: Arc<Mutex<Option<ObserverPlaybackProof>>>,
) -> Result<(LiveContractState, ObserverPlaybackProof)> {
    let mut pause_resume_validated = state.validated_pause && state.validated_resume;
    let mut observer_audio_started = Some(observer_audio_started);
    let mut observer_playback = ObserverPlaybackProof::default();

    loop {
        let maybe_event = events.next().await;
        let event = next_session_event(maybe_event)?;
        let is_playing = event.kind == SessionEventKind::Playing;
        state.observe_event(event, expected_video_id)?;
        update_live_contract_snapshot(&snapshot, &state);

        if is_playing && !pause_resume_validated {
            let pause_at = Instant::now() + PAUSE_AFTER_PLAYBACK_START;
            wait_for_observer_audio_started(&mut observer_audio_started).await?;
            request_observer_speaking_started_proof(&observer_proof_tx).await?;
            tokio::time::sleep_until(pause_at).await;

            controls
                .resume()
                .await
                .context("call ignored Resume while live playback is already playing")?;
            let playing_snapshot = controls
                .state()
                .await
                .context("fetch state after ignored Resume while already playing")?;
            if playing_snapshot.state != SessionState::Playing {
                bail!(
                    "ignored Resume changed state while playback was already playing: {:?}",
                    playing_snapshot.state
                );
            }
            state.mark_invalid_resume_ignored();
            update_live_contract_snapshot(&snapshot, &state);

            let mut pause_proof = start_observer_pause_proof(&observer_proof_tx).await?;
            let pause_started_at = Instant::now();
            begin_observer_pause_proof(&mut pause_proof)?;
            controls
                .pause()
                .await
                .context("call Pause while staying in the Discord voice channel")?;
            controls
                .pause()
                .await
                .context("call ignored redundant Pause while live playback is already paused")?;
            let paused_snapshot = controls
                .state()
                .await
                .context("fetch state after ignored redundant Pause")?;
            if paused_snapshot.state != SessionState::Paused {
                bail!(
                    "ignored redundant Pause changed state while playback was already paused: {:?}",
                    paused_snapshot.state
                );
            }
            let pause_proof = await_observer_pause_proof(pause_proof).await?;
            observer_playback.pause_silence_ms = pause_proof.silence_ms;
            observer_playback.pause_speaking_stopped = pause_proof.gateway_speaking_stopped;
            observer_playback.pause_rtp_silence_observed = pause_proof.rtp_silence_observed;
            *observer_playback_snapshot.lock().unwrap() = Some(observer_playback.clone());
            if !pause_proof.rtp_silence_observed {
                bail!(
                    "observer pause proof did not observe the explicit RTP stop-silence boundary"
                );
            }
            state.mark_pause();
            state.mark_redundant_pause_ignored();
            update_live_contract_snapshot(&snapshot, &state);

            tokio::time::sleep_until(pause_started_at + PAUSE_HOLD_DURATION).await;
            let resume_proof = start_observer_resume_proof(&observer_proof_tx).await?;
            controls
                .resume()
                .await
                .context("call Resume from paused state without Discord voice rejoin")?;
            let resume_proof = await_observer_resume_proof(resume_proof).await?;
            observer_playback.resume_observed_packet_count = resume_proof.observed_packet_count;
            observer_playback.resume_speaking_started = resume_proof.speaking_started;
            observer_playback.resume_decoded_audio_start_ms =
                resume_proof.resume_decoded_audio_start_ms;
            *observer_playback_snapshot.lock().unwrap() = Some(observer_playback.clone());
            state.mark_resume();
            update_live_contract_snapshot(&snapshot, &state);
            pause_resume_validated = true;
        }

        if state.saw_track_ended {
            return Ok((state, observer_playback));
        }
    }
}

async fn wait_for_observer_audio_started(
    observer_audio_started: &mut Option<oneshot::Receiver<()>>,
) -> Result<()> {
    let Some(observer_audio_started) = observer_audio_started.take() else {
        return Ok(());
    };

    timeout(OBSERVER_AUDIO_STARTED_TIMEOUT, observer_audio_started)
        .await
        .context("timed out waiting for observer to receive service audio before Pause")?
        .context("observer audio task ended before proving pre-Pause audio")?;
    Ok(())
}

async fn request_observer_speaking_started_proof(
    observer_proof_tx: &mpsc::Sender<ObserverAudioProofCommand>,
) -> Result<()> {
    let (respond_to, response) = oneshot::channel();
    observer_proof_tx
        .send(ObserverAudioProofCommand::SpeakingStarted { respond_to })
        .await
        .context("request observer pre-Pause Speaking 1 proof")?;

    timeout(
        OBSERVER_SPEAKING_STATE_TIMEOUT + Duration::from_secs(5),
        response,
    )
    .await
    .context("timed out waiting for observer pre-Pause Speaking 1 proof")?
    .context("observer audio task ended before pre-Pause Speaking 1 proof")?
}

async fn start_observer_pause_proof(
    observer_proof_tx: &mpsc::Sender<ObserverAudioProofCommand>,
) -> Result<PendingObserverPauseProof> {
    let (armed, armed_rx) = oneshot::channel();
    let (begin, begin_rx) = oneshot::channel();
    let (respond_to, response) = oneshot::channel();
    observer_proof_tx
        .send(ObserverAudioProofCommand::Pause {
            armed,
            begin: begin_rx,
            respond_to,
        })
        .await
        .context("request observer Pause proof")?;
    timeout(Duration::from_secs(5), armed_rx)
        .await
        .context("timed out arming observer Pause proof")?
        .context("observer Pause proof task ended before arming")?;

    Ok(PendingObserverPauseProof {
        begin: Some(begin),
        response,
    })
}

fn begin_observer_pause_proof(proof: &mut PendingObserverPauseProof) -> Result<()> {
    let begin = proof
        .begin
        .take()
        .context("observer Pause proof was already begun")?;
    begin
        .send(())
        .map_err(|_| anyhow!("observer audio task ended before Pause proof began"))
}

async fn await_observer_pause_proof(
    proof: PendingObserverPauseProof,
) -> Result<ObserverPauseProof> {
    if proof.begin.is_some() {
        bail!("observer Pause proof was awaited before Pause began");
    }

    timeout(
        OBSERVER_SPEAKING_STATE_TIMEOUT + PAUSE_OBSERVER_SILENCE_DURATION + Duration::from_secs(5),
        proof.response,
    )
    .await
    .context("timed out waiting for observer Pause proof")?
    .context("observer audio task ended before Pause proof")?
}

async fn start_observer_resume_proof(
    observer_proof_tx: &mpsc::Sender<ObserverAudioProofCommand>,
) -> Result<oneshot::Receiver<Result<ObserverResumeProof>>> {
    let (armed, armed_rx) = oneshot::channel();
    let (respond_to, response) = oneshot::channel();
    observer_proof_tx
        .send(ObserverAudioProofCommand::Resume { armed, respond_to })
        .await
        .context("request observer Resume proof")?;
    timeout(Duration::from_secs(5), armed_rx)
        .await
        .context("timed out arming observer Resume proof")?
        .context("observer audio task ended before Resume proof was armed")?;

    Ok(response)
}

async fn await_observer_resume_proof(
    response: oneshot::Receiver<Result<ObserverResumeProof>>,
) -> Result<ObserverResumeProof> {
    timeout(
        RESUME_OBSERVER_AUDIO_TIMEOUT + Duration::from_secs(5),
        response,
    )
    .await
    .context("timed out waiting for observer Resume proof")?
    .context("observer audio task ended before Resume proof")?
}

async fn wait_for_initial_voice_ready(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    mut state: LiveContractState,
) -> Result<LiveContractState> {
    loop {
        let maybe_event = events.next().await;
        let event = next_session_event(maybe_event)?;
        let is_voice_ready = event.kind == SessionEventKind::VoiceReady;
        state.observe_event(event, expected_video_id)?;
        if is_voice_ready {
            return Ok(state);
        }
    }
}

async fn validate_ready_state_rpc(
    client: &mut VoiceServiceClient,
    state: &mut LiveContractState,
    snapshot: &Arc<Mutex<LiveContractState>>,
) -> Result<()> {
    let service_state = client
        .state()
        .await
        .context("call GetState through Twilight adapter before Play")?;
    if service_state.state != SessionState::VoiceReady {
        bail!(
            "GetState returned {:?} before Play; expected VoiceReady",
            service_state.state
        );
    }

    state.mark_get_state();
    update_live_contract_snapshot(snapshot, state);
    Ok(())
}

async fn validate_post_play_control_rpcs(
    client: &mut VoiceServiceClient,
    service_addr: &str,
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    voice: &ServiceVoiceContext,
    probe_config: PostPlayProbeConfig<'_>,
    state: &mut LiveContractState,
    snapshot: &Arc<Mutex<LiveContractState>>,
) -> std::result::Result<CompletedPostPlayControlEvidence, Box<PostPlayControlFailure>> {
    let mut evidence = PostPlayControlEvidence::default();
    let expected_video_id = probe_config.expected_video_id;
    let playback_metrics = match fetch_finished_playback_metrics(client, expected_video_id).await {
        Ok(metrics) => metrics,
        Err(error) => return evidence.fail(error),
    };
    let playback_metrics_evidence: PlaybackStabilityEvidence = (&playback_metrics).into();
    evidence.playback_metrics = Some(playback_metrics_evidence.clone());
    if let Err(error) = validate_finished_playback_metrics(&playback_metrics, expected_video_id) {
        return evidence.fail(error);
    }
    state.mark_get_playback_metrics();
    update_live_contract_snapshot(snapshot, state);

    let reconnect_probe_metrics = match validate_active_reconnect_rollover_during_playback(
        client,
        service_addr,
        events,
        voice,
        expected_video_id,
        state,
        snapshot,
    )
    .await
    {
        Ok(metrics) => metrics,
        Err(error) => return evidence.fail(error),
    };
    evidence.reconnect_probe_metrics = Some(reconnect_probe_metrics.clone());

    if let Err(error) = validate_active_stop_during_playback(
        client,
        service_addr,
        events,
        expected_video_id,
        state,
        snapshot,
    )
    .await
    {
        return evidence.fail(error);
    }

    if let Err(error) = validate_active_leave_voice_during_playback(
        client,
        service_addr,
        events,
        expected_video_id,
        state,
        snapshot,
    )
    .await
    {
        return evidence.fail(error);
    }

    if let Err(error) = state.ensure_complete() {
        return evidence.fail(error);
    }
    Ok(CompletedPostPlayControlEvidence {
        playback_metrics: playback_metrics_evidence,
        reconnect_probe_metrics,
    })
}

async fn validate_active_reconnect_rollover_during_playback(
    client: &mut VoiceServiceClient,
    service_addr: &str,
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    voice: &ServiceVoiceContext,
    expected_video_id: &str,
    state: &mut LiveContractState,
    snapshot: &Arc<Mutex<LiveContractState>>,
) -> Result<PlaybackStabilityEvidence> {
    let mut probe_play_client = VoiceServiceClient::connect(service_addr.to_owned())
        .await
        .context("connect active reconnect rollover probe play client")?;
    let probe_video_id = expected_video_id.to_owned();
    let play_task = tokio::spawn(async move {
        probe_play_client
            .play(probe_video_id)
            .await
            .context("call Play for active reconnect rollover probe")
    });

    let mut play_task = wait_for_interrupt_probe_playing_with_play_task(
        events,
        expected_video_id,
        "ReconnectRollover",
        play_task,
    )
    .await?;
    if let Err(error) =
        wait_for_active_interrupt_probe_media(&mut play_task, "ReconnectRollover").await
    {
        cancel_interrupt_probe_play_task(play_task).await;
        return Err(error);
    }
    if let Err(error) = client
        .update_voice_context(voice)
        .await
        .context("call UpdateVoiceContext while live validation probe playback is Playing")
    {
        cancel_interrupt_probe_play_task(play_task).await;
        return Err(error);
    }

    await_interrupt_probe_play_task(play_task, "ReconnectRollover").await?;
    wait_for_reconnect_rollover_probe_resumed(events, expected_video_id).await?;
    let reconnect_probe_metrics = fetch_reconnect_probe_metrics(client, expected_video_id).await?;

    client
        .stop()
        .await
        .context("call Stop after active reconnect rollover probe resumed")?;
    wait_for_interrupt_probe_stopped(events, expected_video_id, "ReconnectRollover").await?;
    let stopped = client
        .state()
        .await
        .context("fetch service state after active reconnect rollover probe")?;
    ensure_state_after_active_stop(&stopped)?;

    state.mark_reconnect_rollover_during_playback();
    update_live_contract_snapshot(snapshot, state);
    Ok((&reconnect_probe_metrics).into())
}

async fn validate_active_stop_during_playback(
    client: &mut VoiceServiceClient,
    service_addr: &str,
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    state: &mut LiveContractState,
    snapshot: &Arc<Mutex<LiveContractState>>,
) -> Result<()> {
    let mut probe_play_client = VoiceServiceClient::connect(service_addr.to_owned())
        .await
        .context("connect active Stop probe play client")?;
    let probe_video_id = expected_video_id.to_owned();
    let play_task = tokio::spawn(async move {
        probe_play_client
            .play(probe_video_id)
            .await
            .context("call Play for active Stop probe")
    });

    let mut play_task = wait_for_interrupt_probe_playing_with_play_task(
        events,
        expected_video_id,
        "Stop",
        play_task,
    )
    .await?;
    if let Err(error) = wait_for_active_interrupt_probe_media(&mut play_task, "Stop").await {
        cancel_interrupt_probe_play_task(play_task).await;
        return Err(error);
    }
    if let Err(error) = client
        .stop()
        .await
        .context("call Stop while live validation probe playback is Playing")
    {
        cancel_interrupt_probe_play_task(play_task).await;
        return Err(error);
    }
    if let Err(error) = wait_for_interrupt_probe_stopped(events, expected_video_id, "Stop").await {
        cancel_interrupt_probe_play_task(play_task).await;
        return Err(error);
    }
    await_interrupt_probe_play_task(play_task, "Stop").await?;

    let stopped = client
        .state()
        .await
        .context("fetch service state after active Stop probe")?;
    ensure_state_after_active_stop(&stopped)?;
    state.mark_stop_during_playback();
    update_live_contract_snapshot(snapshot, state);
    Ok(())
}

async fn validate_active_leave_voice_during_playback(
    client: &mut VoiceServiceClient,
    service_addr: &str,
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    state: &mut LiveContractState,
    snapshot: &Arc<Mutex<LiveContractState>>,
) -> Result<()> {
    let mut probe_play_client = VoiceServiceClient::connect(service_addr.to_owned())
        .await
        .context("connect active LeaveVoice probe play client")?;
    let probe_video_id = expected_video_id.to_owned();
    let play_task = tokio::spawn(async move {
        probe_play_client
            .play(probe_video_id)
            .await
            .context("call Play for active LeaveVoice probe")
    });

    let mut play_task = wait_for_interrupt_probe_playing_with_play_task(
        events,
        expected_video_id,
        "LeaveVoice",
        play_task,
    )
    .await?;
    if let Err(error) = wait_for_active_interrupt_probe_media(&mut play_task, "LeaveVoice").await {
        cancel_interrupt_probe_play_task(play_task).await;
        return Err(error);
    }
    if let Err(error) = client
        .leave_voice()
        .await
        .context("call LeaveVoice while live validation probe playback is Playing")
    {
        cancel_interrupt_probe_play_task(play_task).await;
        return Err(error);
    }
    await_interrupt_probe_play_task(play_task, "LeaveVoice").await?;

    let left = client
        .state()
        .await
        .context("fetch service state after active LeaveVoice probe")?;
    ensure_state_after_active_leave_voice(&left)?;
    state.mark_leave_voice_during_playback();
    update_live_contract_snapshot(snapshot, state);
    Ok(())
}

async fn await_interrupt_probe_play_task(
    play_task: tokio::task::JoinHandle<Result<()>>,
    label: &str,
) -> Result<()> {
    timeout(LIVE_INTERRUPT_PROBE_TIMEOUT, play_task)
        .await
        .with_context(|| {
            format!(
                "timed out waiting for active {label} probe Play RPC to return after interruption"
            )
        })?
        .context("active interrupt probe Play task panicked")?
        .with_context(|| format!("active {label} probe Play RPC failed after interruption"))
}

async fn cancel_interrupt_probe_play_task(play_task: tokio::task::JoinHandle<Result<()>>) {
    play_task.abort();
    let _ = play_task.await;
}

#[cfg(test)]
async fn wait_for_interrupt_probe_playing(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    label: &str,
) -> Result<()> {
    let deadline = Instant::now() + LIVE_INTERRUPT_PROBE_TIMEOUT;
    loop {
        let event = next_session_event_before_deadline(events, deadline, label).await?;
        if validate_interrupt_probe_playing_event(&event, expected_video_id, label)? {
            return Ok(());
        }
    }
}

async fn wait_for_interrupt_probe_playing_with_play_task(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    label: &str,
    mut play_task: tokio::task::JoinHandle<Result<()>>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let deadline = Instant::now() + LIVE_INTERRUPT_PROBE_TIMEOUT;
    loop {
        fail_if_interrupt_probe_play_task_finished(&mut play_task, label).await?;

        let remaining = match deadline.checked_duration_since(Instant::now()) {
            Some(remaining) => remaining,
            None => {
                cancel_interrupt_probe_play_task(play_task).await;
                bail!(
                    "timed out waiting for active {label} probe service events after {} seconds",
                    LIVE_INTERRUPT_PROBE_TIMEOUT.as_secs()
                );
            }
        };
        let event_wait = remaining.min(INTERRUPT_PROBE_PLAY_TASK_POLL_INTERVAL);

        if let Ok(maybe_event) = timeout(event_wait, events.next()).await {
            let event = match next_session_event(maybe_event) {
                Ok(event) => event,
                Err(error) => {
                    cancel_interrupt_probe_play_task(play_task).await;
                    return Err(error);
                }
            };
            match validate_interrupt_probe_playing_event(&event, expected_video_id, label) {
                Ok(true) => return Ok(play_task),
                Ok(false) => {}
                Err(error) => {
                    cancel_interrupt_probe_play_task(play_task).await;
                    return Err(error);
                }
            };
        }
    }
}

async fn fail_if_interrupt_probe_play_task_finished(
    play_task: &mut tokio::task::JoinHandle<Result<()>>,
    label: &str,
) -> Result<()> {
    if !play_task.is_finished() {
        return Ok(());
    }

    let play_result = play_task
        .await
        .context("active interrupt probe Play task panicked")?;
    match play_result {
        Ok(()) => bail!("active {label} probe Play RPC returned before Playing"),
        Err(error) => Err(error)
            .with_context(|| format!("active {label} probe Play RPC failed before Playing")),
    }
}

async fn wait_for_active_interrupt_probe_media(
    play_task: &mut tokio::task::JoinHandle<Result<()>>,
    label: &str,
) -> Result<()> {
    let deadline = Instant::now() + ACTIVE_INTERRUPT_PROBE_MEDIA_SETTLE_DURATION;
    loop {
        fail_if_interrupt_probe_play_task_finished(play_task, label).await?;
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Ok(());
        };
        tokio::time::sleep(remaining.min(INTERRUPT_PROBE_PLAY_TASK_POLL_INTERVAL)).await;
    }
}

fn validate_interrupt_probe_playing_event(
    event: &SessionEvent,
    expected_video_id: &str,
    label: &str,
) -> Result<bool> {
    match event.kind {
        SessionEventKind::TrackResolving
        | SessionEventKind::Buffering
        | SessionEventKind::Playing => {
            validate_interrupt_probe_video_id(event, expected_video_id, label)?;
            Ok(event.kind == SessionEventKind::Playing)
        }
        SessionEventKind::TrackEnded => {
            bail!("active {label} probe reached TrackEnded before interruption");
        }
        SessionEventKind::Stopped => {
            bail!("active {label} probe observed Stopped before issuing the interrupt command");
        }
        SessionEventKind::PlaybackInterrupted => {
            bail!(
                "active {label} probe observed PlaybackInterrupted before issuing the interrupt command: {}",
                display_probe_event_message(event)
            );
        }
        SessionEventKind::FatalError => {
            bail!(
                "active {label} probe observed FatalError before issuing the interrupt command: {}",
                display_probe_event_message(event)
            );
        }
        SessionEventKind::VoiceReconnecting => {
            bail!(
                "active {label} probe observed VoiceReconnecting before issuing the interrupt command: {}",
                display_probe_event_message(event)
            );
        }
        _ => Ok(false),
    }
}

async fn wait_for_interrupt_probe_stopped(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    label: &str,
) -> Result<()> {
    let deadline = Instant::now() + LIVE_INTERRUPT_PROBE_TIMEOUT;
    loop {
        let event = next_session_event_before_deadline(events, deadline, label).await?;
        match event.kind {
            SessionEventKind::Stopped => return Ok(()),
            SessionEventKind::TrackEnded => {
                bail!("active {label} probe reached TrackEnded after Stop");
            }
            SessionEventKind::TrackResolving
            | SessionEventKind::Buffering
            | SessionEventKind::Playing => {
                validate_interrupt_probe_video_id(&event, expected_video_id, label)?;
            }
            SessionEventKind::PlaybackInterrupted => {
                bail!(
                    "active {label} probe observed PlaybackInterrupted after Stop: {}",
                    display_probe_event_message(&event)
                );
            }
            SessionEventKind::FatalError => {
                bail!(
                    "active {label} probe observed FatalError after Stop: {}",
                    display_probe_event_message(&event)
                );
            }
            _ => {}
        }
    }
}

async fn wait_for_reconnect_rollover_probe_resumed(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
) -> Result<()> {
    let deadline = Instant::now() + LIVE_INTERRUPT_PROBE_TIMEOUT;
    let mut saw_reconnecting = false;
    let mut saw_voice_ready = false;

    loop {
        let event =
            next_session_event_before_deadline(events, deadline, "ReconnectRollover").await?;
        match event.kind {
            SessionEventKind::VoiceReconnecting => {
                validate_interrupt_probe_video_id(&event, expected_video_id, "ReconnectRollover")?;
                saw_reconnecting = true;
            }
            SessionEventKind::VoiceReady => {
                validate_interrupt_probe_video_id(&event, expected_video_id, "ReconnectRollover")?;
                if saw_reconnecting {
                    saw_voice_ready = true;
                }
            }
            SessionEventKind::TrackResolving | SessionEventKind::Buffering => {
                validate_interrupt_probe_video_id(&event, expected_video_id, "ReconnectRollover")?;
            }
            SessionEventKind::Playing => {
                validate_interrupt_probe_video_id(&event, expected_video_id, "ReconnectRollover")?;
                if saw_reconnecting && saw_voice_ready {
                    return Ok(());
                }
            }
            SessionEventKind::TrackEnded => {
                bail!("active ReconnectRollover probe reached TrackEnded before resumed playback");
            }
            SessionEventKind::Stopped => {
                bail!("active ReconnectRollover probe observed Stopped before resumed playback");
            }
            SessionEventKind::PlaybackInterrupted => {
                bail!(
                    "active ReconnectRollover probe observed PlaybackInterrupted before resumed playback: {}",
                    display_probe_event_message(&event)
                );
            }
            SessionEventKind::FatalError => {
                bail!(
                    "active ReconnectRollover probe observed FatalError before resumed playback: {}",
                    display_probe_event_message(&event)
                );
            }
            _ => {}
        }
    }
}

async fn next_session_event_before_deadline(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    deadline: Instant,
    label: &str,
) -> Result<SessionEvent> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| {
            anyhow!(
                "timed out waiting for active {label} probe service events after {} seconds",
                LIVE_INTERRUPT_PROBE_TIMEOUT.as_secs()
            )
        })?;
    let maybe_event = timeout(remaining, events.next()).await.map_err(|_| {
        anyhow!(
            "timed out waiting for active {label} probe service events after {} seconds",
            LIVE_INTERRUPT_PROBE_TIMEOUT.as_secs()
        )
    })?;

    next_session_event(maybe_event)
}

fn validate_interrupt_probe_video_id(
    event: &SessionEvent,
    expected_video_id: &str,
    label: &str,
) -> Result<()> {
    if event.current_video_id.as_deref() == Some(expected_video_id) {
        return Ok(());
    }

    bail!(
        "active {label} probe observed {} for current_video_id {:?}; expected `{expected_video_id}`",
        event.kind.as_str_name(),
        event.current_video_id,
    );
}

fn ensure_state_after_active_stop(snapshot: &StateSnapshot) -> Result<()> {
    if snapshot.state != SessionState::VoiceReady {
        bail!(
            "active Stop probe left service in {:?}; expected VoiceReady",
            snapshot.state
        );
    }
    if snapshot.current_video_id.is_some() || snapshot.selected_itag.is_some() {
        bail!(
            "active Stop probe left track metadata in state: current_video_id={:?} selected_itag={:?}",
            snapshot.current_video_id,
            snapshot.selected_itag,
        );
    }

    Ok(())
}

fn ensure_state_after_active_leave_voice(snapshot: &StateSnapshot) -> Result<()> {
    if snapshot.state != SessionState::Idle {
        bail!(
            "active LeaveVoice probe left service in {:?}; expected Idle",
            snapshot.state
        );
    }
    if snapshot.guild_id.is_some()
        || snapshot.channel_id.is_some()
        || snapshot.current_video_id.is_some()
        || snapshot.selected_itag.is_some()
    {
        bail!(
            "active LeaveVoice probe left voice or track metadata in state: guild_id={:?} channel_id={:?} current_video_id={:?} selected_itag={:?}",
            snapshot.guild_id,
            snapshot.channel_id,
            snapshot.current_video_id,
            snapshot.selected_itag,
        );
    }

    Ok(())
}

fn display_probe_event_message(event: &SessionEvent) -> String {
    event
        .message
        .as_deref()
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("no message")
        .to_owned()
}

async fn fetch_finished_playback_metrics(
    client: &mut VoiceServiceClient,
    expected_video_id: &str,
) -> Result<PlaybackStabilitySnapshot> {
    let started_at = Instant::now();
    loop {
        let metrics = client
            .playback_metrics()
            .await
            .context("call GetPlaybackMetrics through Twilight adapter after TrackEnded")?;
        if metrics.available
            && metrics.ended
            && metrics.video_id.as_deref() == Some(expected_video_id)
        {
            return Ok(metrics);
        }

        if started_at.elapsed() >= PLAYBACK_METRICS_TIMEOUT {
            bail!(
                "GetPlaybackMetrics did not return finished metrics for `{expected_video_id}` within {} seconds",
                PLAYBACK_METRICS_TIMEOUT.as_secs()
            );
        }

        tokio::time::sleep(PLAYBACK_METRICS_POLL_INTERVAL).await;
    }
}

async fn fetch_reconnect_probe_metrics(
    client: &mut VoiceServiceClient,
    expected_video_id: &str,
) -> Result<PlaybackStabilitySnapshot> {
    let started_at = Instant::now();
    loop {
        let metrics = client
            .playback_metrics()
            .await
            .context("call GetPlaybackMetrics after active reconnect rollover probe")?;
        if metrics.available
            && !metrics.ended
            && metrics.video_id.as_deref() == Some(expected_video_id)
            && metrics.reconnect_interruptions > 0
        {
            validate_reconnect_probe_metrics(&metrics, expected_video_id)?;
            return Ok(metrics);
        }

        if started_at.elapsed() >= PLAYBACK_METRICS_TIMEOUT {
            bail!(
                "GetPlaybackMetrics did not return active reconnect rollover probe metrics for `{expected_video_id}` within {} seconds",
                PLAYBACK_METRICS_TIMEOUT.as_secs()
            );
        }

        tokio::time::sleep(PLAYBACK_METRICS_POLL_INTERVAL).await;
    }
}

fn validate_finished_playback_metrics(
    metrics: &PlaybackStabilitySnapshot,
    expected_video_id: &str,
) -> Result<()> {
    if !metrics.available {
        bail!("GetPlaybackMetrics returned unavailable metrics after TrackEnded");
    }
    if metrics.video_id.as_deref() != Some(expected_video_id) {
        bail!(
            "GetPlaybackMetrics returned video_id {:?}; expected `{expected_video_id}`",
            metrics.video_id
        );
    }
    if !metrics.ended {
        bail!("GetPlaybackMetrics returned a snapshot that was not marked ended");
    }
    if metrics.track_packet_count < MIN_STABILITY_METRIC_PACKET_COUNT {
        bail!(
            "GetPlaybackMetrics reported only {} track packets; expected at least {}",
            metrics.track_packet_count,
            MIN_STABILITY_METRIC_PACKET_COUNT
        );
    }
    if metrics.track_interval.samples == 0 {
        bail!("GetPlaybackMetrics returned no track RTP interval samples");
    }
    if metrics.sender_lateness.samples == 0 {
        bail!("GetPlaybackMetrics returned no sender lateness samples");
    }
    if metrics.refill_duration.samples == 0 {
        bail!("GetPlaybackMetrics returned no refill duration samples");
    }
    validate_playback_timing_budget(metrics, "finished playback")?;
    validate_live_runtime_post_source_window_count(metrics, "finished playback")?;

    Ok(())
}

fn validate_live_runtime_post_source_window_count(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
) -> Result<()> {
    if metrics.track_tempo_window_post_source_buffer_count < MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS {
        bail!(
            "{label} returned {} runtime post-source tempo windows; expected at least {MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS}",
            metrics.track_tempo_window_post_source_buffer_count
        );
    }
    Ok(())
}

fn validate_source_buffer_target(metrics: &PlaybackStabilitySnapshot, label: &str) -> Result<()> {
    if metrics.source_buffer_target_ms != SOURCE_PLAYBACK_BUFFER_TARGET_MS {
        bail!(
            "{label} returned source_buffer_target_ms {}; expected source buffer target {SOURCE_PLAYBACK_BUFFER_TARGET_MS}",
            metrics.source_buffer_target_ms
        );
    }
    if metrics.adaptive_buffer_target_ms != SOURCE_PLAYBACK_BUFFER_TARGET_MS {
        bail!(
            "{label} returned adaptive_buffer_target_ms {}; expected source buffer target {SOURCE_PLAYBACK_BUFFER_TARGET_MS}",
            metrics.adaptive_buffer_target_ms
        );
    }
    if metrics.max_adaptive_buffer_target_ms != SOURCE_PLAYBACK_BUFFER_TARGET_MS {
        bail!(
            "{label} returned max_adaptive_buffer_target_ms {}; expected source buffer target {SOURCE_PLAYBACK_BUFFER_TARGET_MS}",
            metrics.max_adaptive_buffer_target_ms
        );
    }
    validate_source_buffer_depth_metrics(metrics, label)?;
    Ok(())
}

fn validate_source_buffer_depth_metrics(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
) -> Result<()> {
    let source_depth = metrics
        .source_buffer_depth
        .as_ref()
        .ok_or_else(|| anyhow!("{label} omitted source reservoir depth percentile metrics"))?;
    if source_depth.sample_count == 0 {
        bail!("{label} returned no source reservoir depth samples");
    }
    if source_depth.max_depth.duration_ms == 0 {
        bail!("{label} source reservoir depth never rose above 0ms");
    }
    if source_depth.max_depth.duration_ms < SOURCE_PLAYBACK_BUFFER_TARGET_MS {
        bail!(
            "{label} max source reservoir depth was {}ms; expected at least {SOURCE_PLAYBACK_BUFFER_TARGET_MS}ms",
            source_depth.max_depth.duration_ms
        );
    }
    if source_depth.p50_depth.duration_ms == 0 || source_depth.p95_depth.duration_ms == 0 {
        bail!(
            "{label} source reservoir percentile depths were incomplete (p50={}ms p95={}ms)",
            source_depth.p50_depth.duration_ms,
            source_depth.p95_depth.duration_ms
        );
    }
    Ok(())
}

fn validate_prepared_track_queue_metrics(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
    require_post_resume_phase: bool,
) -> Result<()> {
    if metrics.prepared_track_queue_target_ms != DISCORD_EGRESS_BUFFER_TARGET_MS {
        bail!(
            "{label} returned prepared_track_queue_target_ms {}; expected {DISCORD_EGRESS_BUFFER_TARGET_MS}",
            metrics.prepared_track_queue_target_ms
        );
    }
    if metrics.prepared_track_queue_low_watermark_ms < DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS {
        bail!(
            "{label} returned prepared_track_queue_low_watermark_ms {}; expected >= {DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS}",
            metrics.prepared_track_queue_low_watermark_ms
        );
    }
    if metrics.prepared_track_queue_high_watermark_ms > DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS {
        bail!(
            "{label} returned prepared_track_queue_high_watermark_ms {}; expected <= {DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS}",
            metrics.prepared_track_queue_high_watermark_ms
        );
    }

    let pre_pause = metrics
        .active_pre_pause_prepared_track_queue_depth
        .as_ref()
        .ok_or_else(|| {
            anyhow!("{label} was missing active pre-pause prepared track queue depth metrics")
        })?;
    validate_prepared_track_queue_depth(pre_pause, label, "active pre-pause")?;

    let mut recomputed_sample_count = pre_pause.sample_count;
    let mut recomputed_empty_count = pre_pause.empty_count;
    match metrics
        .active_post_resume_prepared_track_queue_depth
        .as_ref()
    {
        Some(post_resume) => {
            if require_post_resume_phase || post_resume.sample_count != 0 {
                validate_prepared_track_queue_depth(post_resume, label, "active post-resume")?;
            }
            recomputed_sample_count =
                recomputed_sample_count.saturating_add(post_resume.sample_count);
            recomputed_empty_count = recomputed_empty_count.saturating_add(post_resume.empty_count);
        }
        None if require_post_resume_phase => {
            bail!("{label} was missing active post-resume prepared track queue depth metrics");
        }
        None => {}
    }

    if metrics.prepared_track_queue_depth_sample_count != recomputed_sample_count {
        bail!(
            "{label} prepared_track_queue_depth_sample_count was {}; expected recomputed active pre/post sample count {recomputed_sample_count}",
            metrics.prepared_track_queue_depth_sample_count
        );
    }
    if metrics.prepared_track_queue_empty_count != recomputed_empty_count {
        bail!(
            "{label} prepared_track_queue_empty_count was {}; expected recomputed active pre/post empty count {recomputed_empty_count}",
            metrics.prepared_track_queue_empty_count
        );
    }

    Ok(())
}

fn validate_prepared_track_queue_depth(
    depth: &PlaybackQueueDepthStatsSnapshot,
    label: &str,
    phase: &str,
) -> Result<()> {
    if depth.sample_count == 0 {
        bail!("{label} reported no {phase} prepared track queue depth samples");
    }
    if depth.empty_count != 0 {
        bail!(
            "{label} reported {} empty {phase} prepared track queue samples; expected 0",
            depth.empty_count
        );
    }
    if depth.min_depth.duration_ms < PREPARED_TRACK_QUEUE_MIN_DEPTH_MS {
        bail!(
            "{label} {phase} prepared_track_queue_depth_min_ms was {}; expected >= {PREPARED_TRACK_QUEUE_MIN_DEPTH_MS}",
            depth.min_depth.duration_ms
        );
    }
    if depth.p5_depth.duration_ms < PREPARED_TRACK_QUEUE_P5_MIN_DEPTH_MS {
        bail!(
            "{label} {phase} prepared_track_queue_depth_p5_ms was {}; expected >= {PREPARED_TRACK_QUEUE_P5_MIN_DEPTH_MS}",
            depth.p5_depth.duration_ms
        );
    }
    if depth.p50_depth.duration_ms < PREPARED_TRACK_QUEUE_P50_MIN_DEPTH_MS
        || depth.p50_depth.duration_ms > PREPARED_TRACK_QUEUE_P50_MAX_DEPTH_MS
    {
        bail!(
            "{label} {phase} prepared_track_queue_depth_p50_ms was {}; expected between {PREPARED_TRACK_QUEUE_P50_MIN_DEPTH_MS} and {PREPARED_TRACK_QUEUE_P50_MAX_DEPTH_MS}",
            depth.p50_depth.duration_ms
        );
    }
    if depth.p95_depth.duration_ms > DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS {
        bail!(
            "{label} {phase} prepared_track_queue_depth_p95_ms was {}; expected <= {DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS}",
            depth.p95_depth.duration_ms
        );
    }
    if depth.max_depth.duration_ms > DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS {
        bail!(
            "{label} {phase} prepared_track_queue_depth_max_ms was {}; expected <= {DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS}",
            depth.max_depth.duration_ms
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct RawTrackSend {
    packet_index: u64,
    send_started_offset_us: u64,
    duration_us: u64,
    duration_ms: u64,
    duration_samples: u32,
    media_position_ms: u64,
    media_position_samples: u64,
}

#[derive(Debug, Default)]
struct RecomputedRawPlayback {
    track_segment_count: u64,
    track_packet_count: u64,
    track_media_duration_sent_us: u64,
    track_media_duration_sent_ms: u64,
    track_wall_clock_elapsed_ms: u64,
    track_media_to_wall_clock_ratio_ppm: u64,
    track_fast_interval_count: u64,
    track_fast_interval_min_us: u64,
    track_tempo_window_count: u64,
    track_tempo_window_post_source_buffer_count: u64,
    track_tempo_window_min_ratio_ppm: u64,
    track_tempo_window_max_ratio_ppm: u64,
    track_tempo_window_fast_count: u64,
    track_tempo_window_slow_count: u64,
    skipped_source_frame_count: u64,
    skipped_source_duration_ms: u64,
    skipped_source_duration_samples: u64,
    pause_boundary_silence_packet_count: u64,
}

fn validate_raw_playback_evidence(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
    require_pause_resume_evidence: bool,
) -> Result<()> {
    if metrics.raw_send_events.is_empty() {
        bail!("{label} returned no raw send-event evidence");
    }
    if metrics.raw_prepared_track_queue_samples.is_empty() {
        bail!("{label} returned no raw prepared-track queue sample evidence");
    }
    if metrics.raw_prepared_playout_queue_events.is_empty() {
        bail!("{label} returned no raw prepared playout lifecycle evidence");
    }

    validate_raw_send_event_order(metrics, label)?;
    validate_raw_prepared_track_queue_samples(metrics, label, require_pause_resume_evidence)?;
    validate_raw_prepared_playout_queue_events(metrics, label)?;

    let recomputed = recompute_raw_playback(metrics, label)?;
    if recomputed.track_packet_count != metrics.track_packet_count {
        bail!(
            "{label} track_packet_count was {}; raw send events recomputed {}",
            metrics.track_packet_count,
            recomputed.track_packet_count
        );
    }
    if recomputed.track_media_duration_sent_ms != metrics.track_media_duration_sent_ms {
        bail!(
            "{label} track_media_duration_sent_ms was {}; raw send events recomputed {}",
            metrics.track_media_duration_sent_ms,
            recomputed.track_media_duration_sent_ms
        );
    }
    if recomputed.track_wall_clock_elapsed_ms != metrics.track_wall_clock_elapsed_ms {
        bail!(
            "{label} track_wall_clock_elapsed_ms was {}; raw send events recomputed {}",
            metrics.track_wall_clock_elapsed_ms,
            recomputed.track_wall_clock_elapsed_ms
        );
    }
    if !ppm_matches_with_tolerance(
        recomputed.track_media_to_wall_clock_ratio_ppm,
        metrics.track_media_to_wall_clock_ratio_ppm,
    ) {
        bail!(
            "{label} track_media_to_wall_clock_ratio_ppm was {}; raw send events recomputed {} outside ±{RAW_RATIO_RECOMPUTE_TOLERANCE_PPM}ppm tolerance",
            metrics.track_media_to_wall_clock_ratio_ppm,
            recomputed.track_media_to_wall_clock_ratio_ppm
        );
    }
    if recomputed.track_fast_interval_count != metrics.track_fast_interval_count {
        bail!(
            "{label} track_fast_interval_count was {}; raw send events recomputed {}",
            metrics.track_fast_interval_count,
            recomputed.track_fast_interval_count
        );
    }
    if recomputed.track_fast_interval_min_us != metrics.track_fast_interval_min_us {
        bail!(
            "{label} track_fast_interval_min_us was {}; raw send events recomputed {}",
            metrics.track_fast_interval_min_us,
            recomputed.track_fast_interval_min_us
        );
    }
    if recomputed.track_tempo_window_count != metrics.track_tempo_window_count
        || recomputed.track_tempo_window_post_source_buffer_count
            != metrics.track_tempo_window_post_source_buffer_count
        || recomputed.track_tempo_window_fast_count != metrics.track_tempo_window_fast_count
        || recomputed.track_tempo_window_slow_count != metrics.track_tempo_window_slow_count
        || !ppm_matches_with_tolerance(
            recomputed.track_tempo_window_min_ratio_ppm,
            metrics.track_tempo_window_min_ratio_ppm,
        )
        || !ppm_matches_with_tolerance(
            recomputed.track_tempo_window_max_ratio_ppm,
            metrics.track_tempo_window_max_ratio_ppm,
        )
    {
        bail!(
            "{label} rolling tempo aggregate disagreed with raw send events outside ±{RAW_RATIO_RECOMPUTE_TOLERANCE_PPM}ppm tolerance: reported windows={} post_source={} min={} max={} fast={} slow={}, recomputed windows={} post_source={} min={} max={} fast={} slow={}",
            metrics.track_tempo_window_count,
            metrics.track_tempo_window_post_source_buffer_count,
            metrics.track_tempo_window_min_ratio_ppm,
            metrics.track_tempo_window_max_ratio_ppm,
            metrics.track_tempo_window_fast_count,
            metrics.track_tempo_window_slow_count,
            recomputed.track_tempo_window_count,
            recomputed.track_tempo_window_post_source_buffer_count,
            recomputed.track_tempo_window_min_ratio_ppm,
            recomputed.track_tempo_window_max_ratio_ppm,
            recomputed.track_tempo_window_fast_count,
            recomputed.track_tempo_window_slow_count
        );
    }
    if recomputed.skipped_source_frame_count != metrics.skipped_source_frame_count
        || recomputed.skipped_source_duration_ms != metrics.skipped_source_duration_ms
        || recomputed.skipped_source_duration_samples != metrics.skipped_source_duration_samples
    {
        bail!(
            "{label} skipped-source aggregates disagreed with raw source-frame identity: reported frames={} duration_ms={} duration_samples={}, recomputed frames={} duration_ms={} duration_samples={}",
            metrics.skipped_source_frame_count,
            metrics.skipped_source_duration_ms,
            metrics.skipped_source_duration_samples,
            recomputed.skipped_source_frame_count,
            recomputed.skipped_source_duration_ms,
            recomputed.skipped_source_duration_samples
        );
    }
    if require_pause_resume_evidence
        && recomputed.pause_boundary_silence_packet_count < PAUSE_STOP_SILENCE_FRAME_COUNT as u64
    {
        bail!(
            "{label} raw send events contained only {} boundary-silence packets; expected at least {} for Pause",
            recomputed.pause_boundary_silence_packet_count,
            PAUSE_STOP_SILENCE_FRAME_COUNT
        );
    }
    if require_pause_resume_evidence && recomputed.track_segment_count < 2 {
        bail!(
            "{label} raw send events did not resume track media after Pause boundary silence (track_segment_count={} boundary_silence_packets={}); expected track packets before and after Pause",
            recomputed.track_segment_count,
            recomputed.pause_boundary_silence_packet_count
        );
    }
    if require_pause_resume_evidence {
        validate_pause_boundary_spacing(metrics, label)?;
    }
    let recomputed_scheduled_silence_packet_count = metrics
        .raw_send_events
        .iter()
        .filter(|event| event.command_kind == PlaybackSendCommandKind::ScheduledSilence)
        .count() as u64;
    if metrics.scheduled_silence_packet_count != recomputed_scheduled_silence_packet_count {
        bail!(
            "{label} scheduled_silence_packet_count was {}; raw send events recomputed {}",
            metrics.scheduled_silence_packet_count,
            recomputed_scheduled_silence_packet_count
        );
    }

    Ok(())
}

fn ppm_matches_with_tolerance(left: u64, right: u64) -> bool {
    left.abs_diff(right) <= RAW_RATIO_RECOMPUTE_TOLERANCE_PPM
}

fn validate_pause_boundary_spacing(metrics: &PlaybackStabilitySnapshot, label: &str) -> Result<()> {
    let mut current_run = Vec::new();
    for event in &metrics.raw_send_events {
        if event.command_kind == PlaybackSendCommandKind::BoundarySilence {
            current_run.push(event.sent_offset_us);
            if current_run.len() >= PAUSE_STOP_SILENCE_FRAME_COUNT {
                return validate_pause_boundary_run_spacing(&current_run, label);
            }
            continue;
        }
        current_run.clear();
    }

    bail!(
        "{label} raw send events did not contain a consecutive five-packet Pause boundary-silence run"
    );
}

fn validate_pause_boundary_run_spacing(sent_offsets_us: &[u64], label: &str) -> Result<()> {
    for (index, window) in sent_offsets_us
        .windows(2)
        .take(PAUSE_STOP_SILENCE_FRAME_COUNT.saturating_sub(1))
        .enumerate()
    {
        let spacing_ms = window[1].saturating_sub(window[0]) / 1_000;
        if !(PAUSE_BOUNDARY_MIN_SPACING_MS..=PAUSE_BOUNDARY_MAX_SPACING_MS).contains(&spacing_ms) {
            bail!(
                "{label} Pause boundary silence spacing at interval {index} was {spacing_ms}ms; expected {PAUSE_BOUNDARY_MIN_SPACING_MS}..={PAUSE_BOUNDARY_MAX_SPACING_MS}ms"
            );
        }
    }
    Ok(())
}

fn validate_raw_send_event_order(metrics: &PlaybackStabilitySnapshot, label: &str) -> Result<()> {
    let mut previous_sent_offset: Option<u64> = None;
    let mut previous_rtp_sequence: Option<u32> = None;
    let mut previous_rtp_timestamp: Option<u32> = None;
    let mut previous_duration_samples: Option<u32> = None;
    let mut previous_nonce: Option<u32> = None;

    for (index, event) in metrics.raw_send_events.iter().enumerate() {
        if event.packet_index != index as u64 {
            bail!(
                "{label} raw send event packet_index {} was not sequential at index {index}",
                event.packet_index
            );
        }
        if event.command_kind == PlaybackSendCommandKind::Unspecified {
            bail!("{label} raw send event {index} had unspecified command kind");
        }
        if event.sent_offset_us < event.send_started_offset_us {
            bail!("{label} raw send event {index} completed before send start");
        }
        if let Some(previous_sent_offset) = previous_sent_offset
            && event.sent_offset_us < previous_sent_offset
        {
            bail!("{label} raw send event {index} moved backward in send-completion time");
        }
        if let (Some(sequence), Some(timestamp), Some(duration_samples)) = (
            previous_rtp_sequence,
            previous_rtp_timestamp,
            previous_duration_samples,
        ) {
            let expected_sequence = (sequence + 1) & 0xffff;
            let expected_timestamp = timestamp.wrapping_add(duration_samples);
            if event.rtp_sequence != expected_sequence {
                bail!(
                    "{label} raw send event {index} RTP sequence was {}; expected {}",
                    event.rtp_sequence,
                    expected_sequence
                );
            }
            if event.rtp_timestamp != expected_timestamp {
                bail!(
                    "{label} raw send event {index} RTP timestamp was {}; expected {}",
                    event.rtp_timestamp,
                    expected_timestamp
                );
            }
        }
        if let Some(nonce) = event.protection_nonce {
            if let Some(previous) = previous_nonce
                && nonce <= previous
            {
                bail!(
                    "{label} raw send event {index} protection nonce was {nonce}; expected monotonic increase after {previous}"
                );
            }
            previous_nonce = Some(nonce);
        }
        if event.command_kind == PlaybackSendCommandKind::Track {
            if !event.committed_heard_media {
                bail!("{label} raw track send event {index} did not mark heard-media commit");
            }
            if event.source_frame_epoch.is_none()
                || event.source_media_position_ms.is_none()
                || event.source_media_position_samples.is_none()
            {
                bail!("{label} raw track send event {index} lacked source-frame identity");
            }
            if event.media_duration_samples == 0 {
                bail!("{label} raw track send event {index} had zero media duration samples");
            }
        } else if event.committed_heard_media {
            bail!("{label} raw non-track send event {index} committed heard media");
        }

        previous_sent_offset = Some(event.sent_offset_us);
        previous_rtp_sequence = Some(event.rtp_sequence);
        previous_rtp_timestamp = Some(event.rtp_timestamp);
        previous_duration_samples = Some(event.media_duration_samples);
    }

    Ok(())
}

fn recompute_raw_playback(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
) -> Result<RecomputedRawPlayback> {
    let mut recomputed = RecomputedRawPlayback::default();
    let mut segments: Vec<Vec<RawTrackSend>> = Vec::new();
    let mut current_segment: Vec<RawTrackSend> = Vec::new();
    let mut all_tracks: Vec<RawTrackSend> = Vec::new();

    for event in &metrics.raw_send_events {
        match event.command_kind {
            PlaybackSendCommandKind::Track => {
                let media_position_ms = event.source_media_position_ms.ok_or_else(|| {
                    anyhow!("{label} raw track send lacked source_media_position_ms")
                })?;
                let media_position_samples =
                    event.source_media_position_samples.ok_or_else(|| {
                        anyhow!("{label} raw track send lacked source_media_position_samples")
                    })?;
                let track = RawTrackSend {
                    packet_index: event.packet_index,
                    send_started_offset_us: event.send_started_offset_us,
                    duration_us: duration_us_from_samples(event.media_duration_samples),
                    duration_ms: event.media_duration_ms,
                    duration_samples: event.media_duration_samples,
                    media_position_ms,
                    media_position_samples,
                };
                recomputed.track_packet_count = recomputed.track_packet_count.saturating_add(1);
                recomputed.track_media_duration_sent_us = recomputed
                    .track_media_duration_sent_us
                    .saturating_add(track.duration_us);
                current_segment.push(track);
                all_tracks.push(track);
            }
            PlaybackSendCommandKind::BoundarySilence => {
                recomputed.pause_boundary_silence_packet_count = recomputed
                    .pause_boundary_silence_packet_count
                    .saturating_add(1);
                finish_track_segment(&mut segments, &mut current_segment);
            }
            PlaybackSendCommandKind::ScheduledSilence
            | PlaybackSendCommandKind::OtherBoundary
            | PlaybackSendCommandKind::Unspecified => {
                finish_track_segment(&mut segments, &mut current_segment);
            }
        }
    }
    finish_track_segment(&mut segments, &mut current_segment);
    recomputed.track_segment_count = segments.len() as u64;

    let mut wall_clock_elapsed_us = 0u64;
    for segment in &segments {
        if let (Some(first), Some(last)) = (segment.first(), segment.last()) {
            wall_clock_elapsed_us = wall_clock_elapsed_us
                .saturating_add(
                    last.send_started_offset_us
                        .saturating_sub(first.send_started_offset_us),
                )
                .saturating_add(last.duration_us);
        }

        for pair in segment.windows(2) {
            let previous = pair[0];
            let current = pair[1];
            let interval_us = current
                .send_started_offset_us
                .saturating_sub(previous.send_started_offset_us);
            let previous_duration_us = previous.duration_us;
            if interval_us < previous_duration_us {
                recomputed.track_fast_interval_count =
                    recomputed.track_fast_interval_count.saturating_add(1);
                recomputed.track_fast_interval_min_us =
                    if recomputed.track_fast_interval_min_us == 0 {
                        interval_us
                    } else {
                        recomputed.track_fast_interval_min_us.min(interval_us)
                    };
            }
        }

        for window in segment.windows(TRACK_TEMPO_WINDOW_PACKETS) {
            recompute_tempo_window(&mut recomputed, window);
        }
    }

    recomputed.track_media_duration_sent_ms = recomputed.track_media_duration_sent_us / 1_000;
    recomputed.track_wall_clock_elapsed_ms = wall_clock_elapsed_us / 1_000;
    recomputed.track_media_to_wall_clock_ratio_ppm = ratio_ppm_us(
        recomputed.track_media_duration_sent_us,
        wall_clock_elapsed_us,
    );

    for pair in all_tracks.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        let expected_next_position_samples = previous
            .media_position_samples
            .saturating_add(u64::from(previous.duration_samples));
        if current.media_position_samples < expected_next_position_samples {
            bail!(
                "{label} raw source position moved backward or replayed: previous_position_samples={} previous_duration_samples={} expected_next_position_samples={} current_position_samples={}",
                previous.media_position_samples,
                previous.duration_samples,
                expected_next_position_samples,
                current.media_position_samples
            );
        }
        if current.media_position_samples
            > expected_next_position_samples.saturating_add(samples_from_duration_ms(
                SOURCE_POSITION_CONTINUITY_TOLERANCE_MS,
            ))
        {
            let skipped_samples = current
                .media_position_samples
                .saturating_sub(expected_next_position_samples);
            let skipped_ms = duration_ms_from_samples(skipped_samples);
            let expected_next_position_ms =
                duration_ms_from_samples(expected_next_position_samples);
            bail!(
                "{label} sender_source_skipped_ahead: raw source position jumped forward during playback (previous_packet_index={} current_packet_index={} previous_position_ms={} previous_position_samples={} previous_duration_ms={} previous_duration_samples={} expected_next_position_ms={} expected_next_position_samples={} current_position_ms={} current_position_samples={} skipped_source_duration_ms={} skipped_source_duration_samples={} tolerance_ms={} tolerance_samples={})",
                previous.packet_index,
                current.packet_index,
                previous.media_position_ms,
                previous.media_position_samples,
                previous.duration_ms,
                previous.duration_samples,
                expected_next_position_ms,
                expected_next_position_samples,
                current.media_position_ms,
                current.media_position_samples,
                skipped_ms,
                skipped_samples,
                SOURCE_POSITION_CONTINUITY_TOLERANCE_MS,
                samples_from_duration_ms(SOURCE_POSITION_CONTINUITY_TOLERANCE_MS)
            );
        }
    }

    Ok(recomputed)
}

fn finish_track_segment(
    segments: &mut Vec<Vec<RawTrackSend>>,
    current_segment: &mut Vec<RawTrackSend>,
) {
    if !current_segment.is_empty() {
        segments.push(std::mem::take(current_segment));
    }
}

fn recompute_tempo_window(recomputed: &mut RecomputedRawPlayback, window: &[RawTrackSend]) {
    let Some(first) = window.first() else {
        return;
    };
    let Some(last) = window.last() else {
        return;
    };
    let media_duration_us = window.iter().fold(0u64, |total, packet| {
        total.saturating_add(packet.duration_us)
    });
    let wall_clock_duration_us = last
        .send_started_offset_us
        .saturating_sub(first.send_started_offset_us)
        .saturating_add(last.duration_us);
    let ratio = ratio_ppm_us(media_duration_us, wall_clock_duration_us);

    recomputed.track_tempo_window_count = recomputed.track_tempo_window_count.saturating_add(1);
    if first.media_position_samples >= samples_from_duration_ms(SOURCE_PLAYBACK_BUFFER_TARGET_MS) {
        recomputed.track_tempo_window_post_source_buffer_count = recomputed
            .track_tempo_window_post_source_buffer_count
            .saturating_add(1);
    }
    recomputed.track_tempo_window_min_ratio_ppm =
        if recomputed.track_tempo_window_min_ratio_ppm == 0 {
            ratio
        } else {
            recomputed.track_tempo_window_min_ratio_ppm.min(ratio)
        };
    recomputed.track_tempo_window_max_ratio_ppm =
        recomputed.track_tempo_window_max_ratio_ppm.max(ratio);
    if ratio > MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM {
        recomputed.track_tempo_window_fast_count =
            recomputed.track_tempo_window_fast_count.saturating_add(1);
    }
    if ratio < MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM {
        recomputed.track_tempo_window_slow_count =
            recomputed.track_tempo_window_slow_count.saturating_add(1);
    }
}

fn ratio_ppm_us(media_duration_us: u64, wall_clock_duration_us: u64) -> u64 {
    if media_duration_us == 0 || wall_clock_duration_us == 0 {
        return 0;
    }
    ((u128::from(media_duration_us) * 1_000_000) / u128::from(wall_clock_duration_us))
        .try_into()
        .unwrap_or(u64::MAX)
}

fn duration_us_from_samples(duration_samples: u32) -> u64 {
    u64::from(duration_samples).saturating_mul(1_000_000) / 48_000
}

fn duration_ms_from_samples(duration_samples: u64) -> u64 {
    duration_samples.saturating_mul(1_000) / 48_000
}

fn samples_from_duration_ms(duration_ms: u64) -> u64 {
    duration_ms.saturating_mul(48)
}

fn validate_raw_prepared_track_queue_samples(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
    require_post_resume_phase: bool,
) -> Result<()> {
    let mut pre_pause = Vec::new();
    let mut post_resume = Vec::new();

    for (index, sample) in metrics.raw_prepared_track_queue_samples.iter().enumerate() {
        if sample.sample_index != index as u64 {
            bail!(
                "{label} raw prepared queue sample_index {} was not sequential at index {index}",
                sample.sample_index
            );
        }
        match sample.phase {
            PreparedTrackQueueSamplePhase::ActivePrePause => pre_pause.push(sample.depth.clone()),
            PreparedTrackQueueSamplePhase::ActivePostResume => {
                post_resume.push(sample.depth.clone())
            }
            PreparedTrackQueueSamplePhase::Unspecified => {
                bail!("{label} raw prepared queue sample {index} had unspecified phase");
            }
        }
    }

    let recomputed_pre = queue_depth_stats_from_samples(&pre_pause);
    let recomputed_post = queue_depth_stats_from_samples(&post_resume);
    let pre_reported = metrics
        .active_pre_pause_prepared_track_queue_depth
        .as_ref()
        .ok_or_else(|| anyhow!("{label} was missing active pre-pause prepared queue metrics"))?;
    ensure_queue_depth_stats_match(pre_reported, &recomputed_pre, label, "active pre-pause")?;
    match metrics
        .active_post_resume_prepared_track_queue_depth
        .as_ref()
    {
        Some(post_reported) => {
            ensure_queue_depth_stats_match(
                post_reported,
                &recomputed_post,
                label,
                "active post-resume",
            )?;
        }
        None if require_post_resume_phase || recomputed_post.sample_count != 0 => {
            bail!("{label} was missing active post-resume prepared queue metrics");
        }
        None => {}
    }

    let recomputed_sample_count = recomputed_pre
        .sample_count
        .saturating_add(recomputed_post.sample_count);
    if metrics.prepared_track_queue_depth_sample_count != recomputed_sample_count {
        bail!(
            "{label} prepared_track_queue_depth_sample_count was {}; raw samples recomputed {}",
            metrics.prepared_track_queue_depth_sample_count,
            recomputed_sample_count
        );
    }
    let recomputed_empty_count = recomputed_pre
        .empty_count
        .saturating_add(recomputed_post.empty_count);
    if metrics.prepared_track_queue_empty_count != recomputed_empty_count {
        bail!(
            "{label} prepared_track_queue_empty_count was {}; raw samples recomputed {}",
            metrics.prepared_track_queue_empty_count,
            recomputed_empty_count
        );
    }

    Ok(())
}

fn validate_raw_prepared_playout_queue_events(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
) -> Result<()> {
    let mut unrecovered_track_drop_count = 0u64;
    let mut unrecovered_track_drops = Vec::new();
    let mut silence_drop_count = 0u64;
    let mut rebuild_count = 0u64;
    let mut track_enqueue_count = 0u64;
    let mut track_dequeue_count = 0u64;

    for (index, event) in metrics.raw_prepared_playout_queue_events.iter().enumerate() {
        if event.event_index != index as u64 {
            bail!(
                "{label} raw prepared playout event_index {} was not sequential at index {index}",
                event.event_index
            );
        }
        if event.event_kind == PreparedPlayoutQueueEventKind::Unspecified {
            bail!("{label} raw prepared playout event {index} had unspecified event kind");
        }
        if event.command_kind == PlaybackSendCommandKind::Unspecified {
            bail!("{label} raw prepared playout event {index} had unspecified command kind");
        }
        match event.event_kind {
            PreparedPlayoutQueueEventKind::Enqueued => {
                if event.command_kind == PlaybackSendCommandKind::Track {
                    track_enqueue_count = track_enqueue_count.saturating_add(1);
                }
            }
            PreparedPlayoutQueueEventKind::DequeuedToDeadlineSender => {
                if event.command_kind == PlaybackSendCommandKind::Track {
                    track_dequeue_count = track_dequeue_count.saturating_add(1);
                }
            }
            PreparedPlayoutQueueEventKind::DroppedBeforeSend => match event.command_kind {
                PlaybackSendCommandKind::Track => {
                    if prepared_track_drop_counts_as_unrecovered(event) {
                        unrecovered_track_drop_count =
                            unrecovered_track_drop_count.saturating_add(1);
                        if let Some(identity) = prepared_playout_track_drop_identity(event) {
                            unrecovered_track_drops.push(identity);
                        }
                    }
                }
                PlaybackSendCommandKind::ScheduledSilence
                | PlaybackSendCommandKind::BoundarySilence
                | PlaybackSendCommandKind::OtherBoundary => {
                    silence_drop_count = silence_drop_count.saturating_add(1);
                }
                PlaybackSendCommandKind::Unspecified => {}
            },
            PreparedPlayoutQueueEventKind::Rebuilt => {
                rebuild_count = rebuild_count.saturating_add(1);
                if let Some(identity) = prepared_playout_track_drop_identity(event)
                    && let Some(index) = unrecovered_track_drops
                        .iter()
                        .position(|drop| *drop == identity)
                {
                    unrecovered_track_drops.remove(index);
                    unrecovered_track_drop_count = unrecovered_track_drop_count.saturating_sub(1);
                }
            }
            PreparedPlayoutQueueEventKind::Unspecified => {}
        }
        if event.command_kind == PlaybackSendCommandKind::Track
            && event.queue_depth_after.duration_ms > DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS
        {
            bail!(
                "{label} raw prepared playout event {index} reported track queue depth {}ms above {DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS}ms high watermark",
                event.queue_depth_after.duration_ms
            );
        }
    }

    if track_enqueue_count == 0 {
        bail!("{label} returned no raw prepared track enqueue events");
    }
    if track_dequeue_count == 0 {
        bail!("{label} returned no raw prepared track dequeue-to-deadline events");
    }
    if unrecovered_track_drop_count != metrics.prepared_track_packet_drop_count {
        bail!(
            "{label} prepared_track_packet_drop_count was {}; raw lifecycle events recomputed {} unrecovered drops",
            metrics.prepared_track_packet_drop_count,
            unrecovered_track_drop_count
        );
    }
    if silence_drop_count != metrics.prepared_silence_packet_drop_count {
        bail!(
            "{label} prepared_silence_packet_drop_count was {}; raw lifecycle events recomputed {}",
            metrics.prepared_silence_packet_drop_count,
            silence_drop_count
        );
    }
    if rebuild_count != metrics.prepared_packet_rebuild_count {
        bail!(
            "{label} prepared_packet_rebuild_count was {}; raw lifecycle events recomputed {}",
            metrics.prepared_packet_rebuild_count,
            rebuild_count
        );
    }

    Ok(())
}

fn queue_depth_stats_from_samples(
    samples: &[PlaybackBufferDepthSnapshot],
) -> PlaybackQueueDepthStatsSnapshot {
    if samples.is_empty() {
        return PlaybackQueueDepthStatsSnapshot::default();
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable_by_key(|depth| depth.duration_samples);
    PlaybackQueueDepthStatsSnapshot {
        sample_count: samples.len() as u64,
        empty_count: samples
            .iter()
            .filter(|depth| depth.duration_samples == 0)
            .count() as u64,
        current_depth: samples[samples.len() - 1].clone(),
        min_depth: sorted[0].clone(),
        p5_depth: percentile_depth(&sorted, 5),
        p50_depth: percentile_depth(&sorted, 50),
        p95_depth: percentile_depth(&sorted, 95),
        max_depth: sorted[sorted.len() - 1].clone(),
    }
}

fn percentile_depth(
    sorted: &[PlaybackBufferDepthSnapshot],
    percentile: usize,
) -> PlaybackBufferDepthSnapshot {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index].clone()
}

fn ensure_queue_depth_stats_match(
    reported: &PlaybackQueueDepthStatsSnapshot,
    recomputed: &PlaybackQueueDepthStatsSnapshot,
    label: &str,
    phase: &str,
) -> Result<()> {
    if reported != recomputed {
        bail!(
            "{label} {phase} prepared queue aggregate disagreed with raw samples: reported {:?}, recomputed {:?}",
            reported,
            recomputed
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedPlayoutTrackDropIdentity {
    reason: PreparedPlayoutQueueEventReason,
    media_duration_samples: u32,
    source_frame_epoch: u64,
    source_media_position_samples: u64,
    source_media_byte_position: Option<u64>,
}

fn prepared_track_drop_counts_as_unrecovered(
    event: &discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot,
) -> bool {
    !matches!(
        event.reason,
        PreparedPlayoutQueueEventReason::Stop | PreparedPlayoutQueueEventReason::Reconnect
    )
}

fn prepared_playout_track_drop_identity(
    event: &discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot,
) -> Option<PreparedPlayoutTrackDropIdentity> {
    if event.command_kind != PlaybackSendCommandKind::Track {
        return None;
    }

    Some(PreparedPlayoutTrackDropIdentity {
        reason: event.reason,
        media_duration_samples: event.media_duration_samples,
        source_frame_epoch: event.source_frame_epoch?,
        source_media_position_samples: event.source_media_position_samples?,
        source_media_byte_position: event.source_media_byte_position,
    })
}

fn validate_playback_timing_budget(metrics: &PlaybackStabilitySnapshot, label: &str) -> Result<()> {
    validate_playback_timing_budget_with_active_probe_options(metrics, label, true, true)
}

fn validate_active_probe_playback_timing_budget(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
) -> Result<()> {
    validate_playback_timing_budget_with_active_probe_options(metrics, label, false, false)
}

fn validate_playback_timing_budget_with_active_probe_options(
    metrics: &PlaybackStabilitySnapshot,
    label: &str,
    require_rolling_tempo_windows: bool,
    require_pause_resume_evidence: bool,
) -> Result<()> {
    validate_source_buffer_target(metrics, label)?;
    if metrics.track_interval.samples == 0 {
        bail!("{label} returned no track RTP interval samples");
    }
    if metrics.sender_lateness.samples == 0 {
        bail!("{label} returned no sender lateness samples");
    }
    if metrics.playout_sender_lateness.samples == 0 {
        bail!("{label} returned no playout sender lateness samples");
    }
    if metrics.track_media_duration_sent_ms == 0 {
        bail!("{label} returned zero sent track media duration");
    }
    if metrics.track_wall_clock_elapsed_ms == 0 {
        bail!("{label} returned zero track wall-clock elapsed duration");
    }
    if metrics.track_media_to_wall_clock_ratio_ppm == 0 {
        bail!("{label} returned zero track media-to-wall-clock ratio");
    }
    if metrics.expected_track_frame_count == 0 {
        bail!("{label} returned zero expected sender frame count");
    }
    if metrics.sent_track_frame_count != metrics.track_packet_count {
        bail!(
            "{label} sent_track_frame_count was {}; expected track_packet_count {}",
            metrics.sent_track_frame_count,
            metrics.track_packet_count
        );
    }
    let expected_silence_frame_count = metrics
        .continuity_silence_packet_count
        .max(metrics.scheduled_silence_packet_count);
    if metrics.silence_frame_count != expected_silence_frame_count {
        bail!(
            "{label} silence_frame_count was {}; expected max(continuity_silence_packet_count={}, scheduled_silence_packet_count={}) = {expected_silence_frame_count}",
            metrics.silence_frame_count,
            metrics.continuity_silence_packet_count,
            metrics.scheduled_silence_packet_count
        );
    }
    let expected_deficit = metrics
        .expected_track_frame_count
        .saturating_sub(metrics.sent_track_frame_count + metrics.silence_frame_count);
    if metrics.frame_deficit_count != expected_deficit {
        bail!(
            "{label} frame_deficit_count was {}; expected recomputed deficit {expected_deficit} from expected_frames={} sent_track_frames={} silence_frames={}",
            metrics.frame_deficit_count,
            metrics.expected_track_frame_count,
            metrics.sent_track_frame_count,
            metrics.silence_frame_count
        );
    }
    if metrics.frame_deficit_count != 0 {
        bail!(
            "{label} reported {} sender frame deficits (expected_frames={} sent_track_frames={} silence_frames={} dropped_frames={} late_frames={}); expected 0",
            metrics.frame_deficit_count,
            metrics.expected_track_frame_count,
            metrics.sent_track_frame_count,
            metrics.silence_frame_count,
            metrics.dropped_frame_count,
            metrics.late_frame_count
        );
    }
    let expected_dropped_frame_count = metrics
        .prepared_track_packet_drop_count
        .saturating_add(metrics.prepared_silence_packet_drop_count)
        .saturating_add(metrics.egress_dropped_music_frame_count);
    if metrics.dropped_frame_count != expected_dropped_frame_count {
        bail!(
            "{label} dropped_frame_count was {}; expected recomputed dropped frames {expected_dropped_frame_count}",
            metrics.dropped_frame_count
        );
    }
    if metrics.dropped_frame_count != 0 {
        bail!(
            "{label} reported {} dropped sender frames; expected 0",
            metrics.dropped_frame_count
        );
    }
    if metrics.late_frame_count != 0 {
        bail!(
            "{label} reported {} late sender frames; expected 0",
            metrics.late_frame_count
        );
    }
    if metrics.track_media_to_wall_clock_ratio_ppm < MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM {
        bail!(
            "{label} track media-to-wall-clock ratio was {}ppm; expected >= {MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM}ppm",
            metrics.track_media_to_wall_clock_ratio_ppm
        );
    }
    if metrics.track_media_to_wall_clock_ratio_ppm > MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM {
        bail!(
            "{label} track media-to-wall-clock ratio was {}ppm; expected <= {MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM}ppm",
            metrics.track_media_to_wall_clock_ratio_ppm
        );
    }
    if metrics.track_tempo_window_count == 0 {
        if require_rolling_tempo_windows {
            bail!("{label} returned no rolling track tempo windows");
        }
        if metrics.track_packet_count >= TRACK_TEMPO_WINDOW_PACKETS as u64 {
            bail!(
                "{label} returned no rolling track tempo windows despite {} track packets; expected at least one {TRACK_TEMPO_WINDOW_PACKETS}-packet window",
                metrics.track_packet_count
            );
        }
    } else if metrics.track_tempo_window_min_ratio_ppm == 0
        || metrics.track_tempo_window_max_ratio_ppm == 0
    {
        bail!(
            "{label} returned incomplete rolling tempo ratio bounds (min={} max={})",
            metrics.track_tempo_window_min_ratio_ppm,
            metrics.track_tempo_window_max_ratio_ppm
        );
    }
    if metrics.track_media_duration_sent_ms
        >= SOURCE_PLAYBACK_BUFFER_TARGET_MS.saturating_add(1_000)
        && metrics.track_tempo_window_post_source_buffer_count == 0
    {
        bail!(
            "{label} returned no rolling tempo windows starting after the {SOURCE_PLAYBACK_BUFFER_TARGET_MS}ms source reservoir"
        );
    }
    if metrics.track_tempo_window_fast_count != 0 {
        bail!(
            "{label} reported {} faster-than-real-time rolling tempo windows (fastest_ratio_ppm={} media_ms={} wall_us={}); expected 0",
            metrics.track_tempo_window_fast_count,
            metrics.track_tempo_window_fastest_ratio_ppm,
            metrics.track_tempo_window_fastest_media_ms,
            metrics.track_tempo_window_fastest_wall_clock_us
        );
    }
    if metrics.track_tempo_window_slow_count != 0 {
        bail!(
            "{label} reported {} slower-than-real-time rolling tempo windows (slowest_ratio_ppm={} media_ms={} wall_us={}); expected 0",
            metrics.track_tempo_window_slow_count,
            metrics.track_tempo_window_slowest_ratio_ppm,
            metrics.track_tempo_window_slowest_media_ms,
            metrics.track_tempo_window_slowest_wall_clock_us
        );
    }
    if metrics.track_fast_interval_count != 0 {
        bail!(
            "{label} reported {} shortened local track intervals (min_us={}); expected 0",
            metrics.track_fast_interval_count,
            metrics.track_fast_interval_min_us
        );
    }
    if metrics.skipped_source_frame_count != 0 {
        bail!(
            "{label} reported {} skipped source frames; expected 0 for steady playback",
            metrics.skipped_source_frame_count
        );
    }
    if metrics.skipped_source_duration_ms != 0 {
        bail!(
            "{label} reported {}ms skipped source duration; expected 0 for steady playback",
            metrics.skipped_source_duration_ms
        );
    }
    if metrics.skipped_source_duration_samples != 0 {
        bail!(
            "{label} reported {} skipped source samples; expected 0 for steady playback",
            metrics.skipped_source_duration_samples
        );
    }
    if metrics.tempo_rebase_count != 0 {
        bail!(
            "{label} reported {} track tempo rebases; expected 0",
            metrics.tempo_rebase_count
        );
    }
    if metrics.playout_builder_prepare_duration.samples == 0 {
        bail!(
            "{label} returned no playout builder preparation samples; expected RTP packets to be prepared before the live media tick"
        );
    }
    if metrics.sender_send_duration.samples == 0 {
        bail!("{label} returned no sender UDP send duration samples");
    }
    if metrics.sender_loop_non_send_work_duration.samples == 0 {
        bail!("{label} returned no sender non-send work duration samples");
    }
    if metrics.gateway_event_drain_duration.samples == 0 {
        bail!("{label} returned no gateway event drain duration samples");
    }
    if metrics.sender_loop_non_send_work_duration.p99_ms > SENDER_LOOP_NON_SEND_WORK_P99_BUDGET_MS {
        bail!(
            "{label} sender non-send work p99 was {}ms; expected <= {SENDER_LOOP_NON_SEND_WORK_P99_BUDGET_MS}ms",
            metrics.sender_loop_non_send_work_duration.p99_ms
        );
    }
    if metrics.sender_loop_non_send_work_duration.max_ms > SENDER_LOOP_NON_SEND_WORK_P99_BUDGET_MS
        && metrics.sender_lateness.max_ms > SENDER_LATENESS_P99_BUDGET_MS
    {
        bail!(
            "{label} sender non-send work max was {}ms and sender lateness max was {}ms; expected outlier work to preserve <= {SENDER_LATENESS_P99_BUDGET_MS}ms sender lateness",
            metrics.sender_loop_non_send_work_duration.max_ms,
            metrics.sender_lateness.max_ms
        );
    }
    if metrics.prepared_rtp_queue_depth_ms > DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS {
        bail!(
            "{label} prepared_rtp_queue_depth_ms was {}; expected <= {DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS}",
            metrics.prepared_rtp_queue_depth_ms
        );
    }
    if metrics.egress_buffer_target_ms != DISCORD_EGRESS_BUFFER_TARGET_MS {
        bail!(
            "{label} returned egress_buffer_target_ms {}; expected {DISCORD_EGRESS_BUFFER_TARGET_MS}",
            metrics.egress_buffer_target_ms
        );
    }
    validate_prepared_track_queue_metrics(metrics, label, require_pause_resume_evidence)?;
    validate_raw_playback_evidence(metrics, label, require_pause_resume_evidence)?;
    if metrics.max_egress_buffer_depth.duration_ms == 0 {
        bail!(
            "{label} reported no Discord egress buffering; expected bounded raw Opus egress depth"
        );
    }
    if metrics.max_egress_buffer_depth.duration_ms > DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS {
        bail!(
            "{label} max Discord egress depth was {}ms; expected <= {DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS}ms",
            metrics.max_egress_buffer_depth.duration_ms
        );
    }
    if metrics.max_playout_buffer_depth != metrics.max_egress_buffer_depth {
        bail!(
            "{label} playout depth {:?} did not match egress depth {:?}",
            metrics.max_playout_buffer_depth,
            metrics.max_egress_buffer_depth
        );
    }
    if metrics.track_interval.p95_ms > RTP_INTERVAL_P95_BUDGET_MS {
        bail!(
            "{label} RTP interval p95 was {}ms; expected <= {RTP_INTERVAL_P95_BUDGET_MS}ms",
            metrics.track_interval.p95_ms
        );
    }
    if metrics.track_interval.p99_ms > RTP_INTERVAL_P99_BUDGET_MS {
        bail!(
            "{label} RTP interval p99 was {}ms; expected <= {RTP_INTERVAL_P99_BUDGET_MS}ms",
            metrics.track_interval.p99_ms
        );
    }
    if metrics.track_interval.max_ms >= RTP_INTERVAL_MAX_BUDGET_MS {
        bail!(
            "{label} RTP interval max was {}ms; expected < {RTP_INTERVAL_MAX_BUDGET_MS}ms",
            metrics.track_interval.max_ms
        );
    }
    if metrics.sender_lateness.p99_ms > SENDER_LATENESS_P99_BUDGET_MS {
        bail!(
            "{label} sender lateness p99 was {}ms; expected <= {SENDER_LATENESS_P99_BUDGET_MS}ms",
            metrics.sender_lateness.p99_ms
        );
    }
    if metrics.playout_sender_lateness.p99_ms > SENDER_LATENESS_P99_BUDGET_MS {
        bail!(
            "{label} playout sender lateness p99 was {}ms; expected <= {SENDER_LATENESS_P99_BUDGET_MS}ms",
            metrics.playout_sender_lateness.p99_ms
        );
    }
    if metrics.buffer_underrun_count != 0 {
        bail!(
            "{label} reported {} buffer underruns; expected 0",
            metrics.buffer_underrun_count
        );
    }
    if metrics.rebuffer_count != 0 {
        bail!(
            "{label} reported {} rebuffers; expected 0",
            metrics.rebuffer_count
        );
    }
    if metrics.playout_underrun_count != 0 {
        bail!(
            "{label} reported {} playout underruns; expected 0",
            metrics.playout_underrun_count
        );
    }
    if metrics.egress_underrun_count != 0 {
        bail!(
            "{label} reported {} egress underruns; expected 0",
            metrics.egress_underrun_count
        );
    }
    if metrics.source_underrun_count != 0 {
        bail!(
            "{label} reported {} source underruns; expected 0",
            metrics.source_underrun_count
        );
    }
    if metrics.source_underrun_reached_builder_count != 0 {
        bail!(
            "{label} reported {} source underruns reaching the playout builder; expected 0",
            metrics.source_underrun_reached_builder_count
        );
    }
    if metrics.source_underrun_reached_deadline_sender_count != 0 {
        bail!(
            "{label} reported {} source underruns reaching the deadline sender; expected 0",
            metrics.source_underrun_reached_deadline_sender_count
        );
    }
    if metrics.dave_transition_recovery_reached_deadline_sender_count != 0 {
        bail!(
            "{label} reported {} DAVE recoveries reaching the deadline sender; expected 0",
            metrics.dave_transition_recovery_reached_deadline_sender_count
        );
    }
    if metrics.sender_forbidden_work_count != 0 {
        bail!(
            "{label} reported {} forbidden sender work samples; expected 0",
            metrics.sender_forbidden_work_count
        );
    }
    if metrics.stale_dave_send_prevented_count != 0 {
        bail!(
            "{label} reported {} stale DAVE sends prevented; expected 0 for steady playback",
            metrics.stale_dave_send_prevented_count
        );
    }
    if metrics.controlled_media_interruption_count != 0 {
        bail!(
            "{label} reported {} controlled media interruptions; expected 0 for steady playback",
            metrics.controlled_media_interruption_count
        );
    }
    if metrics.scheduler_late_reset_count != 0 {
        bail!(
            "{label} reported {} scheduler-late media clock resets; expected 0",
            metrics.scheduler_late_reset_count
        );
    }
    if metrics.source_underrun_reset_count != 0 {
        bail!(
            "{label} reported {} source-underrun media clock resets; expected 0",
            metrics.source_underrun_reset_count
        );
    }
    if metrics.dave_transition_recovery_reset_count != 0 {
        bail!(
            "{label} reported {} DAVE transition recovery resets; expected 0 for steady playback",
            metrics.dave_transition_recovery_reset_count
        );
    }
    if metrics.max_consecutive_playout_late_packets > MAX_CONSECUTIVE_PLAYOUT_LATE_PACKETS {
        bail!(
            "{label} reported {} consecutive playout-late packets; expected <= {MAX_CONSECUTIVE_PLAYOUT_LATE_PACKETS}",
            metrics.max_consecutive_playout_late_packets
        );
    }
    if metrics.max_consecutive_late_egress_ticks > MAX_CONSECUTIVE_PLAYOUT_LATE_PACKETS {
        bail!(
            "{label} reported {} consecutive late egress ticks; expected <= {MAX_CONSECUTIVE_PLAYOUT_LATE_PACKETS}",
            metrics.max_consecutive_late_egress_ticks
        );
    }
    if metrics.continuity_silence_packet_count != 0 {
        bail!(
            "{label} reported {} continuity silence packets during playback; expected 0",
            metrics.continuity_silence_packet_count
        );
    }
    if metrics.inserted_silence_duration_ms != 0 {
        bail!(
            "{label} reported {}ms of inserted silence during playback; expected 0",
            metrics.inserted_silence_duration_ms
        );
    }
    if metrics.egress_inserted_silence_duration_ms != 0 {
        bail!(
            "{label} reported {}ms of egress inserted silence during playback; expected 0",
            metrics.egress_inserted_silence_duration_ms
        );
    }
    if metrics.scheduled_silence_packet_count != 0 {
        bail!(
            "{label} reported {} scheduled silence packets during steady playback; expected 0",
            metrics.scheduled_silence_packet_count
        );
    }
    if metrics.prepared_silence_packet_drop_count != 0 {
        bail!(
            "{label} reported {} dropped prepared silence packets; expected 0",
            metrics.prepared_silence_packet_drop_count
        );
    }
    if metrics.discarded_source_frame_count != 0
        || metrics.discarded_source_duration_ms != 0
        || metrics.discarded_source_duration_samples != 0
    {
        bail!(
            "{label} reported discarded source frames={} duration_ms={} duration_samples={}; expected 0 for steady playback",
            metrics.discarded_source_frame_count,
            metrics.discarded_source_duration_ms,
            metrics.discarded_source_duration_samples
        );
    }
    if metrics.egress_dropped_music_frame_count != 0
        || metrics.egress_dropped_music_duration_ms != 0
        || metrics.egress_dropped_music_duration_samples != 0
    {
        bail!(
            "{label} reported dropped egress music frames={} duration_ms={} duration_samples={}; expected 0 for steady playback",
            metrics.egress_dropped_music_frame_count,
            metrics.egress_dropped_music_duration_ms,
            metrics.egress_dropped_music_duration_samples
        );
    }
    Ok(())
}

fn validate_reconnect_probe_metrics(
    metrics: &PlaybackStabilitySnapshot,
    expected_video_id: &str,
) -> Result<()> {
    if !metrics.available {
        bail!("Reconnect rollover probe metrics were unavailable");
    }
    if metrics.video_id.as_deref() != Some(expected_video_id) {
        bail!(
            "Reconnect rollover probe metrics returned video_id {:?}; expected `{expected_video_id}`",
            metrics.video_id
        );
    }
    if metrics.ended {
        bail!("Reconnect rollover probe metrics unexpectedly reported ended=true");
    }
    if metrics.reconnect_interruptions == 0 {
        bail!("Reconnect rollover probe metrics did not count a reconnect interruption");
    }
    if metrics.track_packet_count == 0 {
        bail!("Reconnect rollover probe metrics did not observe any track packets before rollover");
    }
    if metrics.sender_lateness.samples == 0 {
        bail!("Reconnect rollover probe metrics returned no sender lateness samples");
    }
    validate_active_probe_playback_timing_budget(metrics, "Reconnect rollover probe playback")?;

    Ok(())
}

async fn cleanup_after_flow(
    service_addr: &str,
    service_joined: bool,
    service: GatewayCleanupTarget<'_>,
    observer: GatewayCleanupTarget<'_>,
) -> Result<()> {
    let service_cleanup = if service_joined {
        cleanup_service_voice(service_addr).await
    } else {
        Ok(())
    };
    let service_gateway_cleanup = cleanup_gateway_voice(
        service.http,
        service.sender,
        service.shard,
        service.guild_id,
        service.user_id,
    )
    .await;
    let observer_gateway_cleanup = cleanup_gateway_voice(
        observer.http,
        observer.sender,
        observer.shard,
        observer.guild_id,
        observer.user_id,
    )
    .await;

    combine_results(
        combine_results(service_cleanup, service_gateway_cleanup),
        observer_gateway_cleanup,
    )
}

async fn cleanup_service_voice(service_addr: &str) -> Result<()> {
    let mut cleanup_client = VoiceServiceClient::connect(service_addr.to_owned())
        .await
        .context("connect cleanup Twilight adapter client")?;

    cleanup_client
        .leave_voice()
        .await
        .context("LeaveVoice cleanup failed through Twilight adapter")?;

    Ok(())
}

pub(crate) async fn cleanup_gateway_voice(
    http: &HttpClient,
    sender: &twilight_gateway::MessageSender,
    shard: &mut Shard,
    guild_id: Id<GuildMarker>,
    user_id: Id<UserMarker>,
) -> Result<()> {
    if let Err(source) = sender.command(&leave_voice_channel(guild_id)) {
        if user_absent_from_guild_voice(http, guild_id, user_id).await? {
            return Ok(());
        }

        return Err(source).context("failed to send gateway leave command");
    }

    wait_for_gateway_leave(http, shard, guild_id, user_id).await
}

async fn wait_for_gateway_leave(
    http: &HttpClient,
    shard: &mut Shard,
    guild_id: Id<GuildMarker>,
    user_id: Id<UserMarker>,
) -> Result<()> {
    let deadline = Instant::now() + GATEWAY_LEAVE_TIMEOUT;
    let mut next_voice_state_poll = Instant::now();

    loop {
        let now = Instant::now();
        if now >= deadline {
            if user_absent_from_guild_voice(http, guild_id, user_id).await? {
                return Ok(());
            }

            bail!("timed out waiting for leave confirmation");
        }

        if now >= next_voice_state_poll {
            if user_absent_from_guild_voice(http, guild_id, user_id).await? {
                return Ok(());
            }

            next_voice_state_poll = now + GATEWAY_LEAVE_POLL_INTERVAL;
        }

        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("timed out waiting for leave confirmation"))?;
        let poll_remaining = next_voice_state_poll.saturating_duration_since(Instant::now());
        let next = match timeout(
            remaining.min(poll_remaining),
            shard.next_event(EventTypeFlags::all()),
        )
        .await
        {
            Ok(next) => next,
            Err(_) => continue,
        };

        let Some(item) = next else {
            if user_absent_from_guild_voice(http, guild_id, user_id).await? {
                return Ok(());
            }

            bail!("gateway shard ended before leave confirmation");
        };

        let event = match item {
            Ok(event) => event,
            Err(source) => {
                if is_fatal_gateway_receive_error(&source) {
                    if user_absent_from_guild_voice(http, guild_id, user_id).await? {
                        return Ok(());
                    }

                    return Err(anyhow!(
                        "fatal gateway receive error during cleanup: {source}"
                    ));
                }

                warn!(error = %source, "transient gateway receive error during cleanup");
                continue;
            }
        };

        if let Event::VoiceStateUpdate(update) = event
            && update.user_id == user_id
            && update.guild_id == Some(guild_id)
            && update.channel_id.is_none()
        {
            return Ok(());
        }
    }
}

pub async fn current_user_absent_from_guild_voice(
    http: &HttpClient,
    guild_id: Id<GuildMarker>,
) -> Result<bool> {
    let response = match http.current_user_voice_state(guild_id).await {
        Ok(response) => response,
        Err(source) => {
            if let HttpErrorType::Response { status, .. } = source.kind()
                && status.get() == 404
            {
                return Ok(true);
            }

            return Err(source).context("query current user voice state during cleanup");
        }
    };
    let status = response.status();

    if status == 404 {
        return leave_confirmed_by_rest_voice_state(status.get(), None);
    }

    if !status.is_success() {
        bail!("current user voice state lookup during cleanup failed with status {status}");
    }

    let voice_state = response
        .model()
        .await
        .context("decode current user voice state during cleanup")?;

    leave_confirmed_by_rest_voice_state(status.get(), Some(&voice_state))
}

pub async fn user_absent_from_guild_voice(
    http: &HttpClient,
    guild_id: Id<GuildMarker>,
    user_id: Id<UserMarker>,
) -> Result<bool> {
    let response = match http.user_voice_state(guild_id, user_id).await {
        Ok(response) => response,
        Err(source) => {
            if let HttpErrorType::Response { status, .. } = source.kind()
                && status.get() == 404
            {
                return Ok(true);
            }

            return Err(source).context("query user voice state during cleanup");
        }
    };
    let status = response.status();

    if status == 404 {
        return leave_confirmed_by_rest_voice_state(status.get(), None);
    }

    if !status.is_success() {
        bail!("user voice state lookup during cleanup failed with status {status}");
    }

    let voice_state = response
        .model()
        .await
        .context("decode user voice state during cleanup")?;

    leave_confirmed_by_rest_voice_state(status.get(), Some(&voice_state))
}

pub fn leave_confirmed_by_rest_voice_state(
    status: u16,
    voice_state: Option<&VoiceState>,
) -> Result<bool> {
    if status == 404 {
        return Ok(true);
    }

    if !(200..300).contains(&status) {
        bail!("voice state lookup failed with status {status}");
    }

    let voice_state =
        voice_state.context("voice state lookup succeeded without a voice state body")?;

    Ok(voice_state.channel_id.is_none())
}

pub fn combine_results(primary: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => {
            Err(anyhow!("{primary:#}; cleanup also failed: {cleanup:#}"))
        }
    }
}

#[derive(Default)]
struct PauseSilenceEvidence {
    packet_count: u64,
    spacing_ms: Vec<u64>,
}

fn active_validation_duration_after_resume_ms(
    audio_stats: &AudioValidationStats,
    observer_playback: &ObserverPlaybackProof,
) -> u64 {
    audio_stats
        .decoded_audio_ms
        .saturating_sub(observer_playback.resume_decoded_audio_start_ms)
}

fn pause_silence_evidence(metrics: Option<&PlaybackStabilityEvidence>) -> PauseSilenceEvidence {
    let Some(metrics) = metrics else {
        return PauseSilenceEvidence::default();
    };

    let mut best_run: Vec<u64> = Vec::new();
    let mut current_run: Vec<u64> = Vec::new();
    for event in &metrics.raw_send_events {
        if event.command_kind == "boundary_silence" {
            current_run.push(event.sent_offset_us);
            continue;
        }
        if current_run.len() > best_run.len() {
            best_run = std::mem::take(&mut current_run);
        } else {
            current_run.clear();
        }
    }
    if current_run.len() > best_run.len() {
        best_run = current_run;
    }

    let spacing_ms = best_run
        .windows(2)
        .map(|window| window[1].saturating_sub(window[0]) / 1_000)
        .collect::<Vec<_>>();

    PauseSilenceEvidence {
        packet_count: best_run.len() as u64,
        spacing_ms,
    }
}

fn build_success_evidence(
    config: &StagingConfig,
    validated: &ValidatedLiveOutcome,
) -> LiveValidationEvidence {
    let pause_silence = pause_silence_evidence(validated.playback_metrics.as_ref());
    LiveValidationEvidence {
        outcome: "success".to_owned(),
        service_uri: config.discord_voice_service_uri.clone(),
        ytmusic_addr: config.discord_voice_service_ytmusic_addr.clone(),
        test_video_id: config.test_video_id.clone(),
        expected_track_duration_ms: validated.expected_duration_ms,
        active_validation_duration_after_resume_ms: active_validation_duration_after_resume_ms(
            &validated.audio_stats,
            &validated.observer_playback,
        ),
        pause_silence_packet_count: pause_silence.packet_count,
        pause_silence_spacing_ms: pause_silence.spacing_ms,
        live_staging_profile: config.live_staging_profile.clone(),
        live_staging_service_cpus: config.live_staging_service_cpus.clone(),
        live_staging_cpu_contention_workers: config.live_staging_cpu_contention_workers,
        live_staging_http_read_delay_ms: config.live_staging_http_read_delay_ms,
        live_staging_http_read_jitter_ms: config.live_staging_http_read_jitter_ms,
        validated_join_voice: validated.live_contract.validated_join_voice,
        validated_update_voice_context: validated.live_contract.validated_update_voice_context,
        validated_play: validated.live_contract.validated_play,
        validated_pause: validated.live_contract.validated_pause,
        validated_resume: validated.live_contract.validated_resume,
        validated_invalid_resume_ignored: validated.live_contract.validated_invalid_resume_ignored,
        validated_redundant_pause_ignored: validated
            .live_contract
            .validated_redundant_pause_ignored,
        observer_proved_pause: validated.live_contract.observer_proved_pause,
        observer_proved_resume: validated.live_contract.observer_proved_resume,
        observer_pause_self_mute_observed: validated.observer_playback.pause_self_mute_observed,
        observer_pause_speaking_stopped: validated.observer_playback.pause_speaking_stopped,
        observer_pause_rtp_silence_observed: validated.observer_playback.pause_rtp_silence_observed,
        observer_resume_speaking_started: validated.observer_playback.resume_speaking_started,
        observer_pause_silence_ms: validated.observer_playback.pause_silence_ms,
        observer_resume_packet_count: validated.observer_playback.resume_observed_packet_count,
        validated_reconnect_rollover_during_playback: validated
            .live_contract
            .validated_reconnect_rollover_during_playback,
        validated_stop: validated.live_contract.validated_stop,
        validated_stop_during_playback: validated.live_contract.validated_stop_during_playback,
        validated_leave_voice: validated.live_contract.validated_leave_voice,
        validated_leave_voice_during_playback: validated
            .live_contract
            .validated_leave_voice_during_playback,
        validated_get_state: validated.live_contract.validated_get_state,
        validated_get_playback_metrics: validated.live_contract.validated_get_playback_metrics,
        validated_subscribe_events: validated.live_contract.validated_subscribe_events,
        saw_voice_connecting: validated.live_contract.saw_voice_connecting,
        saw_voice_ready: validated.live_contract.saw_voice_ready,
        saw_track_resolving: validated.live_contract.saw_track_resolving,
        saw_buffering: validated.live_contract.saw_buffering,
        saw_playing: validated.live_contract.saw_playing,
        saw_paused: validated.live_contract.saw_paused,
        saw_resumed_playing: validated.live_contract.saw_resumed_playing,
        saw_track_ended: validated.live_contract.saw_track_ended,
        observed_packet_count: validated.audio_stats.observed_packet_count,
        decoded_audio_ms: validated.audio_stats.decoded_audio_ms,
        observer_wall_clock_elapsed_ms: validated.audio_stats.wall_clock_elapsed_ms,
        observer_decoded_audio_to_wall_clock_ratio_ppm: validated
            .audio_stats
            .decoded_audio_to_wall_clock_ratio_ppm,
        non_silent_audio_ms: validated.audio_stats.non_silent_audio_ms,
        observer_rtp_inter_arrival: (&validated.audio_stats.rtp_inter_arrival).into(),
        observer_rtp_gap_count_gte_100ms: validated.audio_stats.rtp_gap_count_gte_100ms,
        observer_rtp_fast_interval_count: validated.audio_stats.rtp_fast_interval_count,
        observer_rtp_fast_interval_min_ms: validated.audio_stats.rtp_fast_interval_min_ms,
        observer_rtp_fast_interval_min_us: validated.audio_stats.rtp_fast_interval_min_us,
        observer_rtp_buffering_event_count: validated.audio_stats.rtp_buffering_event_count,
        observer_rtp_buffering_total_us: validated.audio_stats.rtp_buffering_total_us,
        observer_rtp_buffering_max_us: validated.audio_stats.rtp_buffering_max_us,
        observer_rtp_speed_change_total_abs_us: validated.audio_stats.rtp_speed_change_total_abs_us,
        observer_rtp_speed_change_total_fast_us: validated
            .audio_stats
            .rtp_speed_change_total_fast_us,
        observer_rtp_speed_change_total_slow_us: validated
            .audio_stats
            .rtp_speed_change_total_slow_us,
        observer_anomaly_count: validated.audio_stats.observer_anomalies.len() as u64,
        observer_anomalies: validated.audio_stats.observer_anomalies.clone(),
        observer_decoded_audio_tempo_window_count: validated
            .audio_stats
            .decoded_audio_tempo_window_count,
        observer_decoded_audio_tempo_window_post_source_buffer_count: validated
            .audio_stats
            .decoded_audio_tempo_window_post_source_buffer_count,
        observer_decoded_audio_tempo_window_min_ratio_ppm: validated
            .audio_stats
            .decoded_audio_tempo_window_min_ratio_ppm,
        observer_decoded_audio_tempo_window_max_ratio_ppm: validated
            .audio_stats
            .decoded_audio_tempo_window_max_ratio_ppm,
        observer_decoded_audio_tempo_window_fast_count: validated
            .audio_stats
            .decoded_audio_tempo_window_fast_count,
        observer_decoded_audio_tempo_window_fastest_ratio_ppm: validated
            .audio_stats
            .decoded_audio_tempo_window_fastest_ratio_ppm,
        observer_decoded_audio_tempo_window_fastest_media_ms: validated
            .audio_stats
            .decoded_audio_tempo_window_fastest_media_ms,
        observer_decoded_audio_tempo_window_fastest_wall_clock_us: validated
            .audio_stats
            .decoded_audio_tempo_window_fastest_wall_clock_us,
        observer_decoded_audio_tempo_window_slow_count: validated
            .audio_stats
            .decoded_audio_tempo_window_slow_count,
        observer_decoded_audio_tempo_window_slowest_ratio_ppm: validated
            .audio_stats
            .decoded_audio_tempo_window_slowest_ratio_ppm,
        observer_decoded_audio_tempo_window_slowest_media_ms: validated
            .audio_stats
            .decoded_audio_tempo_window_slowest_media_ms,
        observer_decoded_audio_tempo_window_slowest_wall_clock_us: validated
            .audio_stats
            .decoded_audio_tempo_window_slowest_wall_clock_us,
        observer_decoded_audio_short_tempo_window_count: validated
            .audio_stats
            .decoded_audio_short_tempo_window_count,
        observer_decoded_audio_short_tempo_window_fast_count: validated
            .audio_stats
            .decoded_audio_short_tempo_window_fast_count,
        observer_decoded_audio_short_tempo_window_slow_count: validated
            .audio_stats
            .decoded_audio_short_tempo_window_slow_count,
        observer_decoded_audio_short_tempo_window_fastest: validated
            .audio_stats
            .decoded_audio_short_tempo_window_fastest
            .clone(),
        observer_decoded_audio_short_tempo_window_slowest: validated
            .audio_stats
            .decoded_audio_short_tempo_window_slowest
            .clone(),
        dave_transition_count_during_playback: validated
            .playback_metrics
            .as_ref()
            .map_or(0, |metrics| metrics.dave_transition_count_during_playback),
        playback_metrics: validated.playback_metrics.clone(),
        reconnect_probe_metrics: validated.reconnect_probe_metrics.clone(),
        validated_constrained_profile: constrained_profile_configured(config)
            && validated.playback_metrics.is_some(),
        validated_slow_jittery_http: slow_jittery_http_configured(config)
            && validated.playback_metrics.is_some(),
        failure_reason: None,
    }
}

fn build_failure_evidence(
    config: &StagingConfig,
    error: &anyhow::Error,
    snapshot: Option<&FailureEvidenceSnapshot>,
) -> LiveValidationEvidence {
    let live_contract = snapshot.and_then(|value| value.live_contract.as_ref());
    let audio_stats = snapshot.and_then(|value| value.audio_stats.as_ref());
    let observer_playback = snapshot.and_then(|value| value.observer_playback.as_ref());
    let reconnect_probe_metrics = snapshot.and_then(|value| value.reconnect_probe_metrics.clone());
    let playback_metrics = snapshot.and_then(|value| value.playback_metrics.clone());
    let pause_silence = pause_silence_evidence(playback_metrics.as_ref());

    LiveValidationEvidence {
        outcome: "failure".to_owned(),
        service_uri: config.discord_voice_service_uri.clone(),
        ytmusic_addr: config.discord_voice_service_ytmusic_addr.clone(),
        test_video_id: config.test_video_id.clone(),
        expected_track_duration_ms: 0,
        active_validation_duration_after_resume_ms: audio_stats
            .zip(observer_playback)
            .map_or(0, |(stats, proof)| {
                active_validation_duration_after_resume_ms(stats, proof)
            }),
        pause_silence_packet_count: pause_silence.packet_count,
        pause_silence_spacing_ms: pause_silence.spacing_ms,
        live_staging_profile: config.live_staging_profile.clone(),
        live_staging_service_cpus: config.live_staging_service_cpus.clone(),
        live_staging_cpu_contention_workers: config.live_staging_cpu_contention_workers,
        live_staging_http_read_delay_ms: config.live_staging_http_read_delay_ms,
        live_staging_http_read_jitter_ms: config.live_staging_http_read_jitter_ms,
        validated_join_voice: live_contract.is_some_and(|state| state.validated_join_voice),
        validated_update_voice_context: live_contract
            .is_some_and(|state| state.validated_update_voice_context),
        validated_play: live_contract.is_some_and(|state| state.validated_play),
        validated_pause: live_contract.is_some_and(|state| state.validated_pause),
        validated_resume: live_contract.is_some_and(|state| state.validated_resume),
        validated_invalid_resume_ignored: live_contract
            .is_some_and(|state| state.validated_invalid_resume_ignored),
        validated_redundant_pause_ignored: live_contract
            .is_some_and(|state| state.validated_redundant_pause_ignored),
        observer_proved_pause: live_contract.is_some_and(|state| state.observer_proved_pause),
        observer_proved_resume: live_contract.is_some_and(|state| state.observer_proved_resume),
        observer_pause_self_mute_observed: observer_playback
            .is_some_and(|proof| proof.pause_self_mute_observed),
        observer_pause_speaking_stopped: observer_playback
            .is_some_and(|proof| proof.pause_speaking_stopped),
        observer_pause_rtp_silence_observed: observer_playback
            .is_some_and(|proof| proof.pause_rtp_silence_observed),
        observer_resume_speaking_started: observer_playback
            .is_some_and(|proof| proof.resume_speaking_started),
        observer_pause_silence_ms: observer_playback.map_or(0, |proof| proof.pause_silence_ms),
        observer_resume_packet_count: observer_playback
            .map_or(0, |proof| proof.resume_observed_packet_count),
        validated_reconnect_rollover_during_playback: live_contract
            .is_some_and(|state| state.validated_reconnect_rollover_during_playback),
        validated_stop: live_contract.is_some_and(|state| state.validated_stop),
        validated_stop_during_playback: live_contract
            .is_some_and(|state| state.validated_stop_during_playback),
        validated_leave_voice: live_contract.is_some_and(|state| state.validated_leave_voice),
        validated_leave_voice_during_playback: live_contract
            .is_some_and(|state| state.validated_leave_voice_during_playback),
        validated_get_state: live_contract.is_some_and(|state| state.validated_get_state),
        validated_get_playback_metrics: live_contract
            .is_some_and(|state| state.validated_get_playback_metrics),
        validated_subscribe_events: live_contract
            .is_some_and(|state| state.validated_subscribe_events),
        saw_voice_connecting: live_contract.is_some_and(|state| state.saw_voice_connecting),
        saw_voice_ready: live_contract.is_some_and(|state| state.saw_voice_ready),
        saw_track_resolving: live_contract.is_some_and(|state| state.saw_track_resolving),
        saw_buffering: live_contract.is_some_and(|state| state.saw_buffering),
        saw_playing: live_contract.is_some_and(|state| state.saw_playing),
        saw_paused: live_contract.is_some_and(|state| state.saw_paused),
        saw_resumed_playing: live_contract.is_some_and(|state| state.saw_resumed_playing),
        saw_track_ended: live_contract.is_some_and(|state| state.saw_track_ended),
        observed_packet_count: audio_stats.map_or(0, |stats| stats.observed_packet_count),
        decoded_audio_ms: audio_stats.map_or(0, |stats| stats.decoded_audio_ms),
        observer_wall_clock_elapsed_ms: audio_stats.map_or(0, |stats| stats.wall_clock_elapsed_ms),
        observer_decoded_audio_to_wall_clock_ratio_ppm: audio_stats
            .map_or(0, |stats| stats.decoded_audio_to_wall_clock_ratio_ppm),
        non_silent_audio_ms: audio_stats.map_or(0, |stats| stats.non_silent_audio_ms),
        observer_rtp_inter_arrival: audio_stats
            .map(|stats| (&stats.rtp_inter_arrival).into())
            .unwrap_or_default(),
        observer_rtp_gap_count_gte_100ms: audio_stats
            .map_or(0, |stats| stats.rtp_gap_count_gte_100ms),
        observer_rtp_fast_interval_count: audio_stats
            .map_or(0, |stats| stats.rtp_fast_interval_count),
        observer_rtp_fast_interval_min_ms: audio_stats
            .map_or(0, |stats| stats.rtp_fast_interval_min_ms),
        observer_rtp_fast_interval_min_us: audio_stats
            .map_or(0, |stats| stats.rtp_fast_interval_min_us),
        observer_rtp_buffering_event_count: audio_stats
            .map_or(0, |stats| stats.rtp_buffering_event_count),
        observer_rtp_buffering_total_us: audio_stats
            .map_or(0, |stats| stats.rtp_buffering_total_us),
        observer_rtp_buffering_max_us: audio_stats.map_or(0, |stats| stats.rtp_buffering_max_us),
        observer_rtp_speed_change_total_abs_us: audio_stats
            .map_or(0, |stats| stats.rtp_speed_change_total_abs_us),
        observer_rtp_speed_change_total_fast_us: audio_stats
            .map_or(0, |stats| stats.rtp_speed_change_total_fast_us),
        observer_rtp_speed_change_total_slow_us: audio_stats
            .map_or(0, |stats| stats.rtp_speed_change_total_slow_us),
        observer_anomaly_count: audio_stats
            .map_or(0, |stats| stats.observer_anomalies.len() as u64),
        observer_anomalies: audio_stats
            .map_or_else(Vec::new, |stats| stats.observer_anomalies.clone()),
        observer_decoded_audio_tempo_window_count: audio_stats
            .map_or(0, |stats| stats.decoded_audio_tempo_window_count),
        observer_decoded_audio_tempo_window_post_source_buffer_count: audio_stats
            .map_or(0, |stats| {
                stats.decoded_audio_tempo_window_post_source_buffer_count
            }),
        observer_decoded_audio_tempo_window_min_ratio_ppm: audio_stats
            .map_or(0, |stats| stats.decoded_audio_tempo_window_min_ratio_ppm),
        observer_decoded_audio_tempo_window_max_ratio_ppm: audio_stats
            .map_or(0, |stats| stats.decoded_audio_tempo_window_max_ratio_ppm),
        observer_decoded_audio_tempo_window_fast_count: audio_stats
            .map_or(0, |stats| stats.decoded_audio_tempo_window_fast_count),
        observer_decoded_audio_tempo_window_fastest_ratio_ppm: audio_stats.map_or(0, |stats| {
            stats.decoded_audio_tempo_window_fastest_ratio_ppm
        }),
        observer_decoded_audio_tempo_window_fastest_media_ms: audio_stats
            .map_or(0, |stats| stats.decoded_audio_tempo_window_fastest_media_ms),
        observer_decoded_audio_tempo_window_fastest_wall_clock_us: audio_stats.map_or(0, |stats| {
            stats.decoded_audio_tempo_window_fastest_wall_clock_us
        }),
        observer_decoded_audio_tempo_window_slow_count: audio_stats
            .map_or(0, |stats| stats.decoded_audio_tempo_window_slow_count),
        observer_decoded_audio_tempo_window_slowest_ratio_ppm: audio_stats.map_or(0, |stats| {
            stats.decoded_audio_tempo_window_slowest_ratio_ppm
        }),
        observer_decoded_audio_tempo_window_slowest_media_ms: audio_stats
            .map_or(0, |stats| stats.decoded_audio_tempo_window_slowest_media_ms),
        observer_decoded_audio_tempo_window_slowest_wall_clock_us: audio_stats.map_or(0, |stats| {
            stats.decoded_audio_tempo_window_slowest_wall_clock_us
        }),
        observer_decoded_audio_short_tempo_window_count: audio_stats
            .map_or(0, |stats| stats.decoded_audio_short_tempo_window_count),
        observer_decoded_audio_short_tempo_window_fast_count: audio_stats
            .map_or(0, |stats| stats.decoded_audio_short_tempo_window_fast_count),
        observer_decoded_audio_short_tempo_window_slow_count: audio_stats
            .map_or(0, |stats| stats.decoded_audio_short_tempo_window_slow_count),
        observer_decoded_audio_short_tempo_window_fastest: audio_stats
            .and_then(|stats| stats.decoded_audio_short_tempo_window_fastest.clone()),
        observer_decoded_audio_short_tempo_window_slowest: audio_stats
            .and_then(|stats| stats.decoded_audio_short_tempo_window_slowest.clone()),
        dave_transition_count_during_playback: snapshot
            .and_then(|value| value.playback_metrics.as_ref())
            .map_or(0, |metrics| metrics.dave_transition_count_during_playback),
        playback_metrics,
        reconnect_probe_metrics,
        validated_constrained_profile: false,
        validated_slow_jittery_http: false,
        failure_reason: Some(classify_failure_reason(error)),
    }
}

fn constrained_profile_configured(config: &StagingConfig) -> bool {
    matches!(
        config.live_staging_profile.as_str(),
        LIVE_STAGING_PROFILE_CONSTRAINED_GITHUB | LIVE_STAGING_PROFILE_CONSTRAINED_LOCAL
    ) && config
        .live_staging_service_cpus
        .parse::<f64>()
        .is_ok_and(|cpus| cpus.is_finite() && cpus > 0.0 && cpus <= MAX_LIVE_STAGING_SERVICE_CPUS)
        && config.live_staging_cpu_contention_workers >= MIN_LIVE_STAGING_CPU_CONTENTION_WORKERS
}

fn slow_jittery_http_configured(config: &StagingConfig) -> bool {
    config.live_staging_http_read_delay_ms >= MIN_LIVE_STAGING_HTTP_READ_DELAY_MS
        && config.live_staging_http_read_jitter_ms >= MIN_LIVE_STAGING_HTTP_READ_JITTER_MS
}

fn emit_failure_evidence(
    config: &StagingConfig,
    error: &anyhow::Error,
    snapshot: Option<&FailureEvidenceSnapshot>,
) {
    let _ = emit_validation_evidence(&build_failure_evidence(config, error, snapshot));
}

fn finish_with_failure(
    config: &StagingConfig,
    primary: Result<()>,
    cleanup: Result<()>,
    snapshot: Option<&FailureEvidenceSnapshot>,
) -> Result<()> {
    let result = combine_results(primary, cleanup);
    if let Err(error) = &result {
        emit_failure_evidence(config, error, snapshot);
    }
    result
}

fn classify_failure_reason(error: &anyhow::Error) -> String {
    const OBSERVER_AUDIO_FAILURE_REASONS: &[&str] = &[
        "observer_audio_missing_packets",
        "observer_audio_incomplete",
        "observer_audio_missing_timing",
        "observer_audio_tempo_fast",
        "observer_audio_tempo_slow",
        "observer_audio_silent",
        "observer_audio_missing_rtp_intervals",
        "observer_audio_rtp_gap",
        "observer_audio_buffered",
        "observer_audio_speed_changed",
        "observer_audio_missing_tempo_windows",
        "observer_audio_missing_short_tempo_windows",
        "observer_audio_short_tempo_fast",
        "observer_audio_short_tempo_slow",
        "observer_audio_short_tempo_inconsistent",
        "observer_audio_insufficient_post_source_tempo_windows",
        "observer_audio_rtp_jitter",
    ];

    let message = error.to_string().to_lowercase();
    if message.contains("speaking 0")
        || message.contains("microphone speaking")
        || message.contains("speaking indicator")
        || message.contains("self_mute")
        || message.contains("pause silence proof")
    {
        "observer_pause_failed".to_owned()
    } else if message.contains("speaking 1")
        || message.contains("resume proof")
        || message.contains("resumed service audio")
    {
        "observer_resume_failed".to_owned()
    } else if let Some(reason) = OBSERVER_AUDIO_FAILURE_REASONS
        .iter()
        .copied()
        .find(|reason| message.contains(reason))
    {
        reason.to_owned()
    } else if message.contains("observer") && message.contains("timed out") {
        "observer_timeout".to_owned()
    } else if message.contains("sender_source_skipped_ahead")
        || message.contains("skipped source")
        || message.contains("skipped-source")
    {
        "sender_source_skipped_ahead".to_owned()
    } else if message.contains("observer audio proof") && message.contains("thresholds") {
        "observer_audio_incomplete".to_owned()
    } else if message.contains("decode opus packet") || message.contains("analyze observer audio") {
        "observer_decode_failed".to_owned()
    } else if message.contains("speaker mapping") {
        "observer_speaker_mapping_missing".to_owned()
    } else if message.contains("trackended")
        || message.contains("playing")
        || message.contains("paused")
        || message.contains("buffering")
        || message.contains("voiceready")
        || message.contains("event stream")
    {
        "service_contract_failed".to_owned()
    } else if message.contains("cleanup") || message.contains("leave") {
        "cleanup_failed".to_owned()
    } else {
        "live_validation_failed".to_owned()
    }
}

fn next_session_event(
    maybe_event: Option<Result<SessionEvent, tonic::Status>>,
) -> Result<SessionEvent> {
    match maybe_event {
        Some(Ok(event)) => Ok(event),
        Some(Err(error)) => Err(anyhow!("event stream failed: {error}")),
        None => bail!("event stream ended before live contract completed"),
    }
}

pub(crate) fn is_fatal_gateway_receive_error(
    error: &twilight_gateway::error::ReceiveMessageError,
) -> bool {
    matches!(error.kind(), ReceiveMessageErrorType::Reconnect)
}

#[cfg(test)]
mod tests {
    use super::*;
    use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
    use discord_voice_service_twilight::{PreparedPlayoutQueueEventReason, SessionEventKind};
    use discord_voice_service_voice::test_support::{
        OPUS_SILENCE_FRAME, ProtectionContext, RtpPacketBuilder,
    };
    use futures::stream;
    use std::collections::HashMap;

    const FRACTIONAL_TRACK_DURATION_SAMPLES: u32 = 120;

    fn observer_short_window(
        ratio_ppm: u64,
        wall_clock_us: u64,
    ) -> crate::audio::AudioTempoWindowEvidence {
        observer_short_window_with_packets(ratio_ppm, wall_clock_us, 25)
    }

    fn observer_short_window_with_packets(
        ratio_ppm: u64,
        wall_clock_us: u64,
        window_packet_count: u64,
    ) -> crate::audio::AudioTempoWindowEvidence {
        crate::audio::AudioTempoWindowEvidence {
            classification: "post_resume_steady_playback".to_owned(),
            window_packet_count,
            media_ms: window_packet_count.saturating_mul(NATURAL_OPUS_FRAME_DURATION_MS),
            wall_clock_us,
            ratio_ppm,
            first_sequence: 1,
            last_sequence: window_packet_count.try_into().unwrap_or(u16::MAX),
        }
    }

    fn event(kind: SessionEventKind, current_video_id: Option<&str>) -> SessionEvent {
        SessionEvent {
            kind,
            current_video_id: current_video_id.map(str::to_owned),
            ..SessionEvent::default()
        }
    }

    fn duration_stats(
        samples: u64,
        p50_ms: u64,
        p95_ms: u64,
        p99_ms: u64,
        min_ms: u64,
        max_ms: u64,
    ) -> discord_voice_service_twilight::DurationStatsSnapshot {
        discord_voice_service_twilight::DurationStatsSnapshot {
            samples,
            p50_ms,
            p95_ms,
            p99_ms,
            min_ms,
            max_ms,
        }
    }

    fn buffer_depth(
        packets: u64,
        bytes: u64,
        duration_ms: u64,
        duration_samples: u64,
    ) -> discord_voice_service_twilight::PlaybackBufferDepthSnapshot {
        discord_voice_service_twilight::PlaybackBufferDepthSnapshot {
            packets,
            bytes,
            duration_ms,
            duration_samples,
        }
    }

    fn queue_depth_stats(
        sample_count: u64,
        empty_count: u64,
        min_ms: u64,
        p5_ms: u64,
        p50_ms: u64,
        p95_ms: u64,
        max_ms: u64,
    ) -> PlaybackQueueDepthStatsSnapshot {
        PlaybackQueueDepthStatsSnapshot {
            sample_count,
            empty_count,
            current_depth: buffer_depth(p50_ms / 20, 0, p50_ms, p50_ms * 48),
            min_depth: buffer_depth(min_ms / 20, 0, min_ms, min_ms * 48),
            p5_depth: buffer_depth(p5_ms / 20, 0, p5_ms, p5_ms * 48),
            p50_depth: buffer_depth(p50_ms / 20, 0, p50_ms, p50_ms * 48),
            p95_depth: buffer_depth(p95_ms / 20, 0, p95_ms, p95_ms * 48),
            max_depth: buffer_depth(max_ms / 20, 0, max_ms, max_ms * 48),
        }
    }

    fn split_track_packets_for_pause_resume(
        track_packets: u64,
        boundary_packets: u64,
    ) -> (u64, u64) {
        if boundary_packets == 0 || track_packets <= 1 {
            (track_packets, 0)
        } else {
            let pre_pause = track_packets / 2;
            (pre_pause, track_packets - pre_pause)
        }
    }

    fn tempo_windows_for_track_packet_count(track_packets: u64) -> u64 {
        track_packets.saturating_sub(TRACK_TEMPO_WINDOW_PACKETS as u64 - 1)
    }

    fn segmented_track_tempo_window_count(track_packets: u64, boundary_packets: u64) -> u64 {
        let (pre_pause, post_resume) =
            split_track_packets_for_pause_resume(track_packets, boundary_packets);
        tempo_windows_for_track_packet_count(pre_pause)
            .saturating_add(tempo_windows_for_track_packet_count(post_resume))
    }

    fn segmented_post_source_tempo_window_count(
        track_packets: u64,
        boundary_packets: u64,
        track_duration_samples: u32,
    ) -> u64 {
        let (pre_pause, post_resume) =
            split_track_packets_for_pause_resume(track_packets, boundary_packets);
        [(0, pre_pause), (pre_pause, post_resume)]
            .into_iter()
            .map(|(first_track_index, segment_packets)| {
                if segment_packets < TRACK_TEMPO_WINDOW_PACKETS as u64 {
                    return 0;
                }

                (0..=segment_packets - TRACK_TEMPO_WINDOW_PACKETS as u64)
                    .filter(|local_window_index| {
                        let track_index = first_track_index + local_window_index;
                        track_index.saturating_mul(u64::from(track_duration_samples))
                            >= samples_from_duration_ms(SOURCE_PLAYBACK_BUFFER_TARGET_MS)
                    })
                    .count() as u64
            })
            .sum()
    }

    fn raw_send_events(
        track_packets: u64,
        boundary_packets: u64,
    ) -> Vec<discord_voice_service_twilight::PlaybackSendEventSnapshot> {
        raw_send_events_with_track_duration(track_packets, boundary_packets, 960)
    }

    fn raw_send_events_with_track_duration(
        track_packets: u64,
        boundary_packets: u64,
        track_duration_samples: u32,
    ) -> Vec<discord_voice_service_twilight::PlaybackSendEventSnapshot> {
        let mut events = Vec::new();
        let track_duration_us = duration_us_from_samples(track_duration_samples);
        let track_duration_ms = duration_ms_from_samples(u64::from(track_duration_samples));
        let (pre_pause_track_packets, post_resume_track_packets) =
            split_track_packets_for_pause_resume(track_packets, boundary_packets);
        let mut packet_index = 0u64;
        let mut send_offset_us = 0u64;
        let mut rtp_sequence = 0u32;
        let mut rtp_timestamp = 0u32;
        let mut sent_track_packets = 0u64;

        for _ in 0..pre_pause_track_packets {
            let media_position_samples = sent_track_packets * u64::from(track_duration_samples);
            events.push(discord_voice_service_twilight::PlaybackSendEventSnapshot {
                packet_index,
                command_kind: PlaybackSendCommandKind::Track,
                expected_deadline_offset_us: send_offset_us,
                send_started_offset_us: send_offset_us,
                sent_offset_us: send_offset_us,
                media_duration_ms: track_duration_ms,
                media_duration_samples: track_duration_samples,
                rtp_sequence,
                rtp_timestamp,
                protection_nonce: Some(packet_index as u32),
                source_frame_epoch: Some(1),
                source_media_position_ms: Some(duration_ms_from_samples(media_position_samples)),
                source_media_position_samples: Some(media_position_samples),
                source_media_byte_position: Some(sent_track_packets * 100),
                committed_heard_media: true,
            });
            packet_index += 1;
            send_offset_us = send_offset_us.saturating_add(track_duration_us);
            rtp_sequence = (rtp_sequence + 1) & 0xffff;
            rtp_timestamp = rtp_timestamp.wrapping_add(track_duration_samples);
            sent_track_packets += 1;
        }

        for _ in 0..boundary_packets {
            events.push(discord_voice_service_twilight::PlaybackSendEventSnapshot {
                packet_index,
                command_kind: PlaybackSendCommandKind::BoundarySilence,
                expected_deadline_offset_us: send_offset_us,
                send_started_offset_us: send_offset_us,
                sent_offset_us: send_offset_us,
                media_duration_ms: 20,
                media_duration_samples: 960,
                rtp_sequence,
                rtp_timestamp,
                protection_nonce: Some(packet_index as u32),
                source_frame_epoch: None,
                source_media_position_ms: None,
                source_media_position_samples: None,
                source_media_byte_position: None,
                committed_heard_media: false,
            });
            packet_index += 1;
            send_offset_us = send_offset_us.saturating_add(20_000);
            rtp_sequence = (rtp_sequence + 1) & 0xffff;
            rtp_timestamp = rtp_timestamp.wrapping_add(960);
        }

        for _ in 0..post_resume_track_packets {
            let media_position_samples = sent_track_packets * u64::from(track_duration_samples);
            events.push(discord_voice_service_twilight::PlaybackSendEventSnapshot {
                packet_index,
                command_kind: PlaybackSendCommandKind::Track,
                expected_deadline_offset_us: send_offset_us,
                send_started_offset_us: send_offset_us,
                sent_offset_us: send_offset_us,
                media_duration_ms: track_duration_ms,
                media_duration_samples: track_duration_samples,
                rtp_sequence,
                rtp_timestamp,
                protection_nonce: Some(packet_index as u32),
                source_frame_epoch: Some(1),
                source_media_position_ms: Some(duration_ms_from_samples(media_position_samples)),
                source_media_position_samples: Some(media_position_samples),
                source_media_byte_position: Some(sent_track_packets * 100),
                committed_heard_media: true,
            });
            packet_index += 1;
            send_offset_us = send_offset_us.saturating_add(track_duration_us);
            rtp_sequence = (rtp_sequence + 1) & 0xffff;
            rtp_timestamp = rtp_timestamp.wrapping_add(track_duration_samples);
            sent_track_packets += 1;
        }
        events
    }

    fn raw_queue_samples()
    -> Vec<discord_voice_service_twilight::PreparedTrackQueueDepthSampleSnapshot> {
        let mut samples = Vec::new();
        for index in 0..96 {
            samples.push(
                discord_voice_service_twilight::PreparedTrackQueueDepthSampleSnapshot {
                    sample_index: index,
                    phase: PreparedTrackQueueSamplePhase::ActivePrePause,
                    depth: buffer_depth(20, 0, 400, 19_200),
                },
            );
        }
        for index in 0..96 {
            samples.push(
                discord_voice_service_twilight::PreparedTrackQueueDepthSampleSnapshot {
                    sample_index: 96 + index,
                    phase: PreparedTrackQueueSamplePhase::ActivePostResume,
                    depth: buffer_depth(20, 0, 400, 19_200),
                },
            );
        }
        samples
    }

    fn raw_pre_pause_queue_samples()
    -> Vec<discord_voice_service_twilight::PreparedTrackQueueDepthSampleSnapshot> {
        (0..96)
            .map(
                |index| discord_voice_service_twilight::PreparedTrackQueueDepthSampleSnapshot {
                    sample_index: index,
                    phase: PreparedTrackQueueSamplePhase::ActivePrePause,
                    depth: buffer_depth(20, 0, 400, 19_200),
                },
            )
            .collect()
    }

    fn raw_prepared_playout_queue_events(
        track_packets: u64,
    ) -> Vec<discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot> {
        raw_prepared_playout_queue_events_with_track_duration(track_packets, 960)
    }

    fn raw_prepared_playout_queue_events_with_track_duration(
        track_packets: u64,
        track_duration_samples: u32,
    ) -> Vec<discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot> {
        let mut events = Vec::new();
        let track_duration_ms = duration_ms_from_samples(u64::from(track_duration_samples));
        let enqueue_depth = buffer_depth(160, 0, 400, 19_200);
        let dequeue_depth = buffer_depth(159, 0, 397, 19_080);
        for index in 0..track_packets {
            let media_position_samples = index * u64::from(track_duration_samples);
            events.push(
                discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot {
                    event_index: events.len() as u64,
                    event_kind: PreparedPlayoutQueueEventKind::Enqueued,
                    reason: PreparedPlayoutQueueEventReason::SteadyPlayback,
                    command_kind: PlaybackSendCommandKind::Track,
                    media_duration_ms: track_duration_ms,
                    media_duration_samples: track_duration_samples,
                    rtp_sequence: index as u32,
                    rtp_timestamp: media_position_samples as u32,
                    protection_nonce: Some(index as u32),
                    source_frame_epoch: Some(1),
                    source_media_position_ms: Some(duration_ms_from_samples(
                        media_position_samples,
                    )),
                    source_media_position_samples: Some(media_position_samples),
                    source_media_byte_position: Some(index * 100),
                    queue_depth_after: enqueue_depth.clone(),
                },
            );
            events.push(
                discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot {
                    event_index: events.len() as u64,
                    event_kind: PreparedPlayoutQueueEventKind::DequeuedToDeadlineSender,
                    reason: PreparedPlayoutQueueEventReason::SteadyPlayback,
                    command_kind: PlaybackSendCommandKind::Track,
                    media_duration_ms: track_duration_ms,
                    media_duration_samples: track_duration_samples,
                    rtp_sequence: index as u32,
                    rtp_timestamp: media_position_samples as u32,
                    protection_nonce: Some(index as u32),
                    source_frame_epoch: Some(1),
                    source_media_position_ms: Some(duration_ms_from_samples(
                        media_position_samples,
                    )),
                    source_media_position_samples: Some(media_position_samples),
                    source_media_byte_position: Some(index * 100),
                    queue_depth_after: dequeue_depth.clone(),
                },
            );
        }
        events
    }

    fn prepared_track_lifecycle_event(
        event_index: u64,
        event_kind: PreparedPlayoutQueueEventKind,
        reason: PreparedPlayoutQueueEventReason,
        source_media_position_samples: u64,
    ) -> discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot {
        discord_voice_service_twilight::PreparedPlayoutQueueEventSnapshot {
            event_index,
            event_kind,
            reason,
            command_kind: PlaybackSendCommandKind::Track,
            media_duration_ms: 2,
            media_duration_samples: 120,
            rtp_sequence: 42,
            rtp_timestamp: source_media_position_samples as u32,
            protection_nonce: Some(42),
            source_frame_epoch: Some(7),
            source_media_position_ms: Some(duration_ms_from_samples(source_media_position_samples)),
            source_media_position_samples: Some(source_media_position_samples),
            source_media_byte_position: Some(source_media_position_samples / 120 * 32),
            queue_depth_after: buffer_depth(0, 0, 0, 0),
        }
    }

    fn valid_playback_timing_snapshot() -> PlaybackStabilitySnapshot {
        let egress_depth = buffer_depth(20, 10_240, DISCORD_EGRESS_BUFFER_TARGET_MS, 19_200);
        let source_depth = queue_depth_stats(
            96,
            0,
            SOURCE_PLAYBACK_BUFFER_TARGET_MS,
            SOURCE_PLAYBACK_BUFFER_TARGET_MS,
            SOURCE_PLAYBACK_BUFFER_TARGET_MS,
            SOURCE_PLAYBACK_BUFFER_TARGET_MS,
            SOURCE_PLAYBACK_BUFFER_TARGET_MS,
        );
        let pre_pause_queue_depth = queue_depth_stats(96, 0, 400, 400, 400, 400, 400);
        let post_resume_queue_depth = queue_depth_stats(96, 0, 400, 400, 400, 400, 400);
        PlaybackStabilitySnapshot {
            available: true,
            playback_epoch: 1,
            video_id: Some("video".to_owned()),
            selected_itag: Some(250),
            track_packet_count: 144,
            track_interval: duration_stats(143, 20, 22, 25, 19, 28),
            track_media_duration_sent_ms: 2_880,
            track_wall_clock_elapsed_ms: 2_880,
            track_media_to_wall_clock_ratio_ppm: 1_000_000,
            expected_track_frame_count: 144,
            sent_track_frame_count: 144,
            silence_frame_count: 0,
            frame_deficit_count: 0,
            dropped_frame_count: 0,
            late_frame_count: 0,
            track_tempo_window_count: segmented_track_tempo_window_count(
                144,
                PAUSE_STOP_SILENCE_FRAME_COUNT as u64,
            ),
            track_tempo_window_min_ratio_ppm: 1_000_000,
            track_tempo_window_max_ratio_ppm: 1_000_000,
            sender_lateness: duration_stats(144, 0, 2, 4, 0, 5),
            max_playout_buffer_depth: egress_depth.clone(),
            egress_buffer_target_ms: DISCORD_EGRESS_BUFFER_TARGET_MS,
            max_egress_buffer_depth: egress_depth,
            source_buffer_target_ms: SOURCE_PLAYBACK_BUFFER_TARGET_MS,
            adaptive_buffer_target_ms: SOURCE_PLAYBACK_BUFFER_TARGET_MS,
            max_adaptive_buffer_target_ms: SOURCE_PLAYBACK_BUFFER_TARGET_MS,
            source_buffer_depth: Some(source_depth),
            sender_send_duration: duration_stats(144, 0, 1, 2, 0, 2),
            sender_loop_non_send_work_duration: duration_stats(144, 0, 1, 1, 0, 1),
            playout_sender_lateness: duration_stats(144, 0, 2, 4, 0, 5),
            playout_builder_prepare_duration: duration_stats(144, 0, 1, 1, 0, 1),
            prepared_rtp_queue_depth_ms: DISCORD_EGRESS_BUFFER_TARGET_MS - 20,
            prepared_track_queue_target_ms: DISCORD_EGRESS_BUFFER_TARGET_MS,
            prepared_track_queue_low_watermark_ms: DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS,
            prepared_track_queue_high_watermark_ms: DISCORD_EGRESS_BUFFER_HIGH_WATERMARK_MS,
            active_pre_pause_prepared_track_queue_depth: Some(pre_pause_queue_depth),
            active_post_resume_prepared_track_queue_depth: Some(post_resume_queue_depth),
            prepared_track_queue_depth_sample_count: 192,
            prepared_track_queue_empty_count: 0,
            raw_send_events: raw_send_events(144, PAUSE_STOP_SILENCE_FRAME_COUNT as u64),
            raw_prepared_track_queue_samples: raw_queue_samples(),
            raw_prepared_playout_queue_events: raw_prepared_playout_queue_events(144),
            gateway_event_drain_duration: duration_stats(144, 0, 1, 2, 0, 2),
            ..PlaybackStabilitySnapshot::default()
        }
    }

    fn active_probe_playback_snapshot(track_packets: u64) -> PlaybackStabilitySnapshot {
        let pre_pause_queue_depth = queue_depth_stats(96, 0, 400, 400, 400, 400, 400);
        let track_media_duration_ms = track_packets.saturating_mul(NATURAL_OPUS_FRAME_DURATION_MS);
        let mut metrics = PlaybackStabilitySnapshot {
            reconnect_interruptions: 1,
            track_packet_count: track_packets,
            track_interval: duration_stats(track_packets.saturating_sub(1), 20, 22, 25, 19, 28),
            track_media_duration_sent_ms: track_media_duration_ms,
            track_wall_clock_elapsed_ms: track_media_duration_ms,
            track_media_to_wall_clock_ratio_ppm: 1_000_000,
            expected_track_frame_count: track_packets,
            sent_track_frame_count: track_packets,
            track_tempo_window_count: 0,
            track_tempo_window_min_ratio_ppm: 0,
            track_tempo_window_max_ratio_ppm: 0,
            sender_lateness: duration_stats(track_packets, 0, 2, 4, 0, 5),
            sender_send_duration: duration_stats(track_packets, 0, 1, 2, 0, 2),
            sender_loop_non_send_work_duration: duration_stats(track_packets, 0, 1, 1, 0, 1),
            playout_sender_lateness: duration_stats(track_packets, 0, 2, 4, 0, 5),
            playout_builder_prepare_duration: duration_stats(track_packets, 0, 1, 1, 0, 1),
            active_pre_pause_prepared_track_queue_depth: Some(pre_pause_queue_depth),
            active_post_resume_prepared_track_queue_depth: Some(
                PlaybackQueueDepthStatsSnapshot::default(),
            ),
            prepared_track_queue_depth_sample_count: 96,
            prepared_track_queue_empty_count: 0,
            raw_send_events: raw_send_events(track_packets, 0),
            raw_prepared_track_queue_samples: raw_pre_pause_queue_samples(),
            raw_prepared_playout_queue_events: raw_prepared_playout_queue_events(track_packets),
            gateway_event_drain_duration: duration_stats(track_packets, 0, 1, 2, 0, 2),
            ..valid_playback_timing_snapshot()
        };
        if track_packets >= TRACK_TEMPO_WINDOW_PACKETS as u64 {
            let tempo_window_count = track_packets - TRACK_TEMPO_WINDOW_PACKETS as u64 + 1;
            metrics.track_tempo_window_count = tempo_window_count;
            metrics.track_tempo_window_min_ratio_ppm = 1_000_000;
            metrics.track_tempo_window_max_ratio_ppm = 1_000_000;
        }
        metrics
    }

    fn assert_playback_timing_budget_rejects(
        mutate: impl FnOnce(&mut PlaybackStabilitySnapshot),
        expected_message: &str,
    ) {
        let mut metrics = valid_playback_timing_snapshot();
        mutate(&mut metrics);

        let error = validate_playback_timing_budget(&metrics, "finished playback")
            .expect_err("playback timing budget should reject the mutated metrics");

        assert!(
            error.to_string().contains(expected_message),
            "expected error to contain `{expected_message}`, got: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_requires_source_buffer_depth_percentiles() {
        let mut metrics = valid_playback_timing_snapshot();
        metrics.source_buffer_depth = None;

        let error = validate_playback_timing_budget(&metrics, "unit playback")
            .expect_err("missing source reservoir percentile metrics should fail");

        assert!(
            error
                .to_string()
                .contains("source reservoir depth percentile metrics"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn interrupt_probe_waits_for_playing_with_expected_video() {
        let mut events = stream::iter([
            Ok(event(SessionEventKind::TrackResolving, Some("video"))),
            Ok(event(SessionEventKind::Buffering, Some("video"))),
            Ok(event(SessionEventKind::Playing, Some("video"))),
        ]);

        wait_for_interrupt_probe_playing(&mut events, "video", "Stop")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn interrupt_probe_rejects_wrong_video_before_interrupt() {
        let mut events = stream::iter([Ok(event(
            SessionEventKind::Playing,
            Some("different-video"),
        ))]);

        let error = wait_for_interrupt_probe_playing(&mut events, "video", "Stop")
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("expected `video`"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn interrupt_probe_reports_play_failure_before_playing_timeout() {
        let mut events = stream::pending::<Result<SessionEvent, tonic::Status>>();
        let play_task = tokio::spawn(async { Err(anyhow!("resolve failed")) });

        let error = wait_for_interrupt_probe_playing_with_play_task(
            &mut events,
            "video",
            "LongTrack",
            play_task,
        )
        .await
        .unwrap_err();

        let error = error.to_string();
        assert!(
            error.contains("active LongTrack probe Play RPC failed before Playing"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn interrupt_probe_rejects_track_end_after_stop() {
        let mut events = stream::iter([Ok(event(SessionEventKind::TrackEnded, Some("video")))]);

        let error = wait_for_interrupt_probe_stopped(&mut events, "video", "Stop")
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("reached TrackEnded after Stop"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn reconnect_rollover_probe_waits_for_reconnect_ready_and_resumed_playing() {
        let mut events = stream::iter([
            Ok(event(SessionEventKind::VoiceReconnecting, Some("video"))),
            Ok(event(SessionEventKind::VoiceReady, Some("video"))),
            Ok(event(SessionEventKind::TrackResolving, Some("video"))),
            Ok(event(SessionEventKind::Buffering, Some("video"))),
            Ok(event(SessionEventKind::Playing, Some("video"))),
        ]);

        wait_for_reconnect_rollover_probe_resumed(&mut events, "video")
            .await
            .unwrap();
    }

    #[test]
    fn reconnect_probe_metrics_require_reconnect_counter() {
        let metrics = PlaybackStabilitySnapshot {
            reconnect_interruptions: 1,
            ..valid_playback_timing_snapshot()
        };
        validate_reconnect_probe_metrics(&metrics, "video").unwrap();

        let without_reconnect = PlaybackStabilitySnapshot {
            reconnect_interruptions: 0,
            ..metrics
        };
        let error = validate_reconnect_probe_metrics(&without_reconnect, "video").unwrap_err();
        assert!(
            error.to_string().contains("reconnect interruption"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn reconnect_probe_metrics_apply_strict_playback_budget() {
        let metrics = PlaybackStabilitySnapshot {
            reconnect_interruptions: 1,
            expected_track_frame_count: 145,
            frame_deficit_count: 1,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_reconnect_probe_metrics(&metrics, "video").unwrap_err();
        assert!(
            error.to_string().contains("sender frame deficits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn reconnect_probe_metrics_allow_short_probe_without_rolling_tempo_window() {
        let metrics = active_probe_playback_snapshot(TRACK_TEMPO_WINDOW_PACKETS as u64 - 1);

        validate_reconnect_probe_metrics(&metrics, "video").unwrap();
    }

    #[test]
    fn reconnect_probe_metrics_require_rolling_tempo_windows_once_probe_is_long_enough() {
        let metrics = PlaybackStabilitySnapshot {
            track_tempo_window_count: 0,
            track_tempo_window_min_ratio_ppm: 0,
            track_tempo_window_max_ratio_ppm: 0,
            ..active_probe_playback_snapshot(TRACK_TEMPO_WINDOW_PACKETS as u64)
        };

        let error = validate_reconnect_probe_metrics(&metrics, "video").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no rolling track tempo windows despite 50 track packets"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_requires_rolling_tempo_windows_for_finished_playback() {
        let metrics = PlaybackStabilitySnapshot {
            track_tempo_window_count: 0,
            track_tempo_window_min_ratio_ppm: 0,
            track_tempo_window_max_ratio_ppm: 0,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("no rolling track tempo windows"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_allows_gateway_drain_max_outlier_when_lateness_is_bounded() {
        let metrics = PlaybackStabilitySnapshot {
            sender_lateness: duration_stats(6_133, 0, 0, 0, 0, 1),
            playout_sender_lateness: duration_stats(6_133, 0, 0, 0, 0, 1),
            sender_loop_non_send_work_duration: duration_stats(6_133, 2, 2, 3, 0, 21),
            gateway_event_drain_duration: duration_stats(6_133, 2, 2, 3, 0, 21),
            ..valid_playback_timing_snapshot()
        };

        validate_playback_timing_budget(&metrics, "finished playback").unwrap();
    }

    #[test]
    fn playback_timing_budget_rejects_sustained_sender_non_send_work() {
        let metrics = PlaybackStabilitySnapshot {
            sender_loop_non_send_work_duration: duration_stats(144, 12, 15, 16, 0, 21),
            gateway_event_drain_duration: duration_stats(144, 12, 15, 16, 0, 21),
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("sender non-send work p99"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_non_send_outlier_that_delays_sender() {
        let metrics = PlaybackStabilitySnapshot {
            sender_lateness: duration_stats(144, 0, 2, 4, 0, 11),
            sender_loop_non_send_work_duration: duration_stats(144, 2, 2, 3, 0, 21),
            gateway_event_drain_duration: duration_stats(144, 2, 2, 3, 0, 21),
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("sender non-send work max"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_missing_prepared_track_queue_metrics() {
        let metrics = PlaybackStabilitySnapshot {
            active_pre_pause_prepared_track_queue_depth: None,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing active pre-pause prepared track queue depth metrics"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_low_prepared_track_queue_watermark() {
        let metrics = PlaybackStabilitySnapshot {
            prepared_track_queue_low_watermark_ms: DISCORD_EGRESS_BUFFER_LOW_WATERMARK_MS - 1,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("prepared_track_queue_low_watermark_ms"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_pre_pause_prepared_track_queue_bounds() {
        let metrics = PlaybackStabilitySnapshot {
            active_pre_pause_prepared_track_queue_depth: Some(queue_depth_stats(
                96, 0, 180, 300, 400, 400, 400,
            )),
            ..valid_playback_timing_snapshot()
        };
        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("active pre-pause prepared_track_queue_depth_min_ms"),
            "unexpected error: {error}"
        );

        let metrics = PlaybackStabilitySnapshot {
            active_pre_pause_prepared_track_queue_depth: Some(queue_depth_stats(
                96, 0, 220, 280, 400, 400, 400,
            )),
            ..valid_playback_timing_snapshot()
        };
        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("active pre-pause prepared_track_queue_depth_p5_ms"),
            "unexpected error: {error}"
        );

        let metrics = PlaybackStabilitySnapshot {
            active_pre_pause_prepared_track_queue_depth: Some(queue_depth_stats(
                96, 0, 220, 320, 320, 400, 400,
            )),
            ..valid_playback_timing_snapshot()
        };
        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("active pre-pause prepared_track_queue_depth_p50_ms"),
            "unexpected error: {error}"
        );

        let metrics = PlaybackStabilitySnapshot {
            active_pre_pause_prepared_track_queue_depth: Some(queue_depth_stats(
                96, 0, 220, 320, 400, 520, 520,
            )),
            ..valid_playback_timing_snapshot()
        };
        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("active pre-pause prepared_track_queue_depth_p95_ms"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_post_resume_prepared_track_queue_bounds() {
        let metrics = PlaybackStabilitySnapshot {
            active_post_resume_prepared_track_queue_depth: Some(queue_depth_stats(
                96, 0, 180, 300, 400, 400, 400,
            )),
            ..valid_playback_timing_snapshot()
        };
        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("active post-resume prepared_track_queue_depth_min_ms"),
            "unexpected error: {error}"
        );

        let metrics = PlaybackStabilitySnapshot {
            active_post_resume_prepared_track_queue_depth: Some(queue_depth_stats(
                96, 0, 220, 320, 480, 500, 500,
            )),
            ..valid_playback_timing_snapshot()
        };
        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("active post-resume prepared_track_queue_depth_p50_ms"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_empty_prepared_track_queue_samples() {
        let metrics = PlaybackStabilitySnapshot {
            active_post_resume_prepared_track_queue_depth: Some(queue_depth_stats(
                96, 1, 220, 320, 400, 400, 400,
            )),
            prepared_track_queue_empty_count: 1,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("empty active post-resume prepared track queue samples"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_missing_raw_send_events() {
        let metrics = PlaybackStabilitySnapshot {
            raw_send_events: Vec::new(),
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("no raw send-event evidence"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_burst_pause_boundary_silence() {
        let mut metrics = valid_playback_timing_snapshot();
        let burst_start_offset_us = metrics
            .raw_send_events
            .iter()
            .find(|event| event.command_kind == PlaybackSendCommandKind::BoundarySilence)
            .map(|event| event.sent_offset_us)
            .expect("valid snapshot should include boundary silence");
        let mut boundary_packet_count = 0;
        for event in metrics
            .raw_send_events
            .iter_mut()
            .filter(|event| event.command_kind == PlaybackSendCommandKind::BoundarySilence)
            .take(PAUSE_STOP_SILENCE_FRAME_COUNT)
        {
            let burst_offset_us = burst_start_offset_us + (boundary_packet_count as u64 * 1_000);
            event.expected_deadline_offset_us = burst_offset_us;
            event.send_started_offset_us = burst_offset_us;
            event.sent_offset_us = burst_offset_us;
            boundary_packet_count += 1;
        }
        assert_eq!(boundary_packet_count, PAUSE_STOP_SILENCE_FRAME_COUNT);

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("Pause boundary silence spacing"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_missing_raw_queue_samples() {
        let metrics = PlaybackStabilitySnapshot {
            raw_prepared_track_queue_samples: Vec::new(),
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no raw prepared-track queue sample evidence"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_missing_raw_playout_lifecycle_events() {
        let metrics = PlaybackStabilitySnapshot {
            raw_prepared_playout_queue_events: Vec::new(),
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no raw prepared playout lifecycle evidence"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_raw_queue_aggregate_disagreement() {
        let mut metrics = valid_playback_timing_snapshot();
        metrics.raw_prepared_track_queue_samples[0]
            .depth
            .duration_ms = 420;

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("prepared queue aggregate disagreed with raw samples"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_raw_playout_lifecycle_aggregate_disagreement() {
        let metrics = PlaybackStabilitySnapshot {
            prepared_track_packet_drop_count: 1,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("dropped_frame_count"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_allows_rebuilt_prepared_track_drops() {
        let mut metrics = valid_playback_timing_snapshot();
        let event_index = metrics.raw_prepared_playout_queue_events.len() as u64;
        metrics
            .raw_prepared_playout_queue_events
            .push(prepared_track_lifecycle_event(
                event_index,
                PreparedPlayoutQueueEventKind::DroppedBeforeSend,
                PreparedPlayoutQueueEventReason::Pause,
                120,
            ));
        metrics
            .raw_prepared_playout_queue_events
            .push(prepared_track_lifecycle_event(
                event_index + 1,
                PreparedPlayoutQueueEventKind::Rebuilt,
                PreparedPlayoutQueueEventReason::Pause,
                120,
            ));
        metrics.prepared_packet_rebuild_count = 1;

        validate_playback_timing_budget(&metrics, "finished playback").unwrap();
    }

    #[test]
    fn playback_timing_budget_allows_controlled_stop_prepared_track_drains() {
        let mut metrics = valid_playback_timing_snapshot();
        let event_index = metrics.raw_prepared_playout_queue_events.len() as u64;
        metrics
            .raw_prepared_playout_queue_events
            .push(prepared_track_lifecycle_event(
                event_index,
                PreparedPlayoutQueueEventKind::DroppedBeforeSend,
                PreparedPlayoutQueueEventReason::Stop,
                120,
            ));
        metrics.restored_source_frame_count = 1;
        metrics.restored_source_duration_ms = 2;
        metrics.restored_source_duration_samples = 120;
        metrics.reconnect_interruptions = 1;

        validate_reconnect_probe_metrics(&metrics, "video").unwrap();
    }

    #[test]
    fn playback_timing_budget_rejects_unrecovered_prepared_track_drops() {
        let mut metrics = valid_playback_timing_snapshot();
        let event_index = metrics.raw_prepared_playout_queue_events.len() as u64;
        metrics
            .raw_prepared_playout_queue_events
            .push(prepared_track_lifecycle_event(
                event_index,
                PreparedPlayoutQueueEventKind::DroppedBeforeSend,
                PreparedPlayoutQueueEventReason::Pause,
                120,
            ));
        metrics.prepared_track_packet_drop_count = 1;
        metrics.dropped_frame_count = 1;
        metrics.expected_track_frame_count = metrics.expected_track_frame_count.saturating_add(1);
        metrics.frame_deficit_count = 1;

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("sender frame deficits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_raw_tempo_aggregate_disagreement() {
        let metrics = PlaybackStabilitySnapshot {
            track_tempo_window_max_ratio_ppm: 999_997,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("rolling tempo aggregate disagreed with raw send events"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_tempo_rebase_count() {
        let metrics = PlaybackStabilitySnapshot {
            tempo_rebase_count: 1,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error.to_string().contains("track tempo rebases"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_named_sender_anomaly_counters() {
        assert_playback_timing_budget_rejects(
            |metrics| metrics.track_fast_interval_count = 1,
            "shortened local track intervals",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.track_tempo_window_fast_count = 1;
                metrics.track_tempo_window_max_ratio_ppm = MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM + 1;
            },
            "faster-than-real-time rolling tempo windows",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.track_tempo_window_slow_count = 1;
                metrics.track_tempo_window_min_ratio_ppm = MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM - 1;
            },
            "slower-than-real-time rolling tempo windows",
        );
        assert_playback_timing_budget_rejects(
            |metrics| metrics.scheduler_late_reset_count = 1,
            "scheduler-late media clock resets",
        );
        assert_playback_timing_budget_rejects(
            |metrics| metrics.source_underrun_count = 1,
            "source underruns",
        );
        assert_playback_timing_budget_rejects(
            |metrics| metrics.source_underrun_reached_builder_count = 1,
            "source underruns reaching the playout builder",
        );
        assert_playback_timing_budget_rejects(
            |metrics| metrics.source_underrun_reached_deadline_sender_count = 1,
            "source underruns reaching the deadline sender",
        );
        assert_playback_timing_budget_rejects(|metrics| metrics.rebuffer_count = 1, "rebuffers");
        assert_playback_timing_budget_rejects(
            |metrics| metrics.continuity_silence_packet_count = 1,
            "silence_frame_count was 0; expected max",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.inserted_silence_duration_ms = 20;
                metrics.egress_inserted_silence_duration_ms = 20;
            },
            "inserted silence during playback",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.scheduled_silence_packet_count = 1;
                metrics.silence_frame_count = 1;
                let last = metrics
                    .raw_send_events
                    .last()
                    .expect("valid snapshot should include raw send events")
                    .clone();
                metrics.raw_send_events.push(
                    discord_voice_service_twilight::PlaybackSendEventSnapshot {
                        packet_index: last.packet_index + 1,
                        command_kind: PlaybackSendCommandKind::ScheduledSilence,
                        expected_deadline_offset_us: last.expected_deadline_offset_us + 20_000,
                        send_started_offset_us: last.send_started_offset_us + 20_000,
                        sent_offset_us: last.sent_offset_us + 20_000,
                        media_duration_ms: NATURAL_OPUS_FRAME_DURATION_MS,
                        media_duration_samples: NATURAL_OPUS_FRAME_DURATION_SAMPLES,
                        rtp_sequence: (last.rtp_sequence + 1) & 0xffff,
                        rtp_timestamp: last.rtp_timestamp.wrapping_add(last.media_duration_samples),
                        protection_nonce: last.protection_nonce.map(|nonce| nonce + 1),
                        source_frame_epoch: None,
                        source_media_position_ms: None,
                        source_media_position_samples: None,
                        source_media_byte_position: None,
                        committed_heard_media: false,
                    },
                );
            },
            "scheduled silence packets during steady playback",
        );
        assert_playback_timing_budget_rejects(
            |metrics| metrics.late_frame_count = 1,
            "late sender frames",
        );
    }

    #[test]
    fn playback_timing_budget_rejects_sender_frame_deficits_and_drops() {
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.expected_track_frame_count += 1;
                metrics.frame_deficit_count = 1;
            },
            "sender frame deficits",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.prepared_track_packet_drop_count = 1;
                metrics.dropped_frame_count = 1;
            },
            "dropped sender frames",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.egress_dropped_music_frame_count = 1;
                metrics.egress_dropped_music_duration_ms = 20;
                metrics.egress_dropped_music_duration_samples = 960;
                metrics.dropped_frame_count = 1;
            },
            "dropped sender frames",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.discarded_source_frame_count = 1;
                metrics.discarded_source_duration_ms = 20;
                metrics.discarded_source_duration_samples = 960;
            },
            "discarded source frames",
        );
    }

    #[test]
    fn playback_timing_budget_rejects_skipped_source_frames_and_speed_like_durations() {
        assert_playback_timing_budget_rejects(
            |metrics| {
                metrics.skipped_source_frame_count = 1;
                metrics.skipped_source_duration_samples =
                    u64::from(NATURAL_OPUS_FRAME_DURATION_SAMPLES);
                metrics.skipped_source_duration_ms = NATURAL_OPUS_FRAME_DURATION_MS;
            },
            "skipped source frames",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                let event = &mut metrics.raw_send_events[10];
                event.media_duration_ms = 0;
                event.media_duration_samples = 0;
            },
            "zero media duration samples",
        );
        assert_playback_timing_budget_rejects(
            |metrics| {
                let previous_position = metrics.raw_send_events[41].source_media_position_samples;
                let event = &mut metrics.raw_send_events[42];
                event.source_media_position_samples = previous_position;
                event.source_media_position_ms = event
                    .source_media_position_samples
                    .map(duration_ms_from_samples);
            },
            "raw source position moved backward or replayed",
        );
    }

    #[test]
    fn playback_timing_budget_rejects_post_resume_sender_source_skip() {
        let mut metrics = valid_playback_timing_snapshot();
        let boundary_run_end = metrics
            .raw_send_events
            .iter()
            .position(|event| event.command_kind == PlaybackSendCommandKind::BoundarySilence)
            .map(|first_boundary| first_boundary + PAUSE_STOP_SILENCE_FRAME_COUNT)
            .expect("valid snapshot should include Pause boundary silence");
        let post_resume_index = metrics
            .raw_send_events
            .iter()
            .enumerate()
            .skip(boundary_run_end)
            .find_map(|(index, event)| {
                (event.command_kind == PlaybackSendCommandKind::Track).then_some(index)
            })
            .expect("valid snapshot should include post-resume track media");
        let skipped_samples = samples_from_duration_ms(PAUSE_HOLD_DURATION.as_millis() as u64);
        let post_resume = &mut metrics.raw_send_events[post_resume_index];
        post_resume.source_media_position_samples = post_resume
            .source_media_position_samples
            .map(|position| position + skipped_samples);
        post_resume.source_media_position_ms = post_resume
            .source_media_position_samples
            .map(duration_ms_from_samples);

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("sender_source_skipped_ahead"),
            "unexpected error: {error}"
        );
        assert!(
            message.contains("skipped_source_duration_ms=3000"),
            "unexpected error: {error}"
        );
        assert!(
            message.contains(&format!("current_packet_index={post_resume_index}")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_rejects_source_underrun_reset_count() {
        let metrics = PlaybackStabilitySnapshot {
            source_underrun_reset_count: 1,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_playback_timing_budget(&metrics, "finished playback").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source-underrun media clock resets"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn playback_timing_budget_allows_one_ppm_raw_tempo_recompute_rounding() {
        let metrics = PlaybackStabilitySnapshot {
            track_tempo_window_max_ratio_ppm: 999_999,
            ..valid_playback_timing_snapshot()
        };

        validate_playback_timing_budget(&metrics, "finished playback").unwrap();
    }

    #[test]
    fn playback_timing_budget_allows_two_ppm_raw_ratio_recompute_rounding() {
        let metrics = PlaybackStabilitySnapshot {
            track_media_to_wall_clock_ratio_ppm: 1_000_002,
            ..valid_playback_timing_snapshot()
        };

        validate_playback_timing_budget(&metrics, "finished playback").unwrap();
    }

    #[test]
    fn raw_playback_recompute_rejects_sub_ms_ratio_drift_hidden_by_ms_metrics() {
        let mut metrics = valid_playback_timing_snapshot();
        let last = metrics
            .raw_send_events
            .iter_mut()
            .rev()
            .find(|event| event.command_kind == PlaybackSendCommandKind::Track)
            .expect("valid snapshot should include track sends");
        last.expected_deadline_offset_us = last.expected_deadline_offset_us.saturating_add(999);
        last.send_started_offset_us = last.send_started_offset_us.saturating_add(999);
        last.sent_offset_us = last.sent_offset_us.saturating_add(999);

        let recomputed = recompute_raw_playback(&metrics, "finished playback").unwrap();

        assert_eq!(recomputed.track_wall_clock_elapsed_ms, 2_880);
        assert_eq!(
            recomputed.track_wall_clock_elapsed_ms,
            metrics.track_wall_clock_elapsed_ms
        );
        assert_eq!(recomputed.track_media_to_wall_clock_ratio_ppm, 999_653);
        assert_ne!(
            recomputed.track_media_to_wall_clock_ratio_ppm,
            metrics.track_media_to_wall_clock_ratio_ppm
        );

        let error = validate_playback_timing_budget(&metrics, "finished playback")
            .expect_err("raw send evidence should reject drift hidden by millisecond metrics");
        assert!(
            error
                .to_string()
                .contains("track_media_to_wall_clock_ratio_ppm"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn raw_playback_recompute_uses_samples_for_fractional_packet_duration() {
        let metrics = PlaybackStabilitySnapshot {
            raw_send_events: (0..TRACK_TEMPO_WINDOW_PACKETS as u64)
                .map(|index| {
                    let sent_offset_us = index * 2_500;
                    discord_voice_service_twilight::PlaybackSendEventSnapshot {
                        packet_index: index,
                        command_kind: PlaybackSendCommandKind::Track,
                        expected_deadline_offset_us: sent_offset_us,
                        send_started_offset_us: sent_offset_us,
                        sent_offset_us,
                        media_duration_ms: 2,
                        media_duration_samples: 120,
                        rtp_sequence: index as u32,
                        rtp_timestamp: (index * 120) as u32,
                        protection_nonce: Some(index as u32),
                        source_frame_epoch: Some(1),
                        source_media_position_ms: Some((index * 2_500) / 1_000),
                        source_media_position_samples: Some(index * 120),
                        source_media_byte_position: Some(index * 32),
                        committed_heard_media: true,
                    }
                })
                .collect(),
            ..Default::default()
        };

        let recomputed = recompute_raw_playback(&metrics, "fractional playback").unwrap();

        assert_eq!(
            recomputed.track_packet_count,
            TRACK_TEMPO_WINDOW_PACKETS as u64
        );
        assert_eq!(recomputed.track_media_duration_sent_ms, 125);
        assert_eq!(recomputed.track_wall_clock_elapsed_ms, 125);
        assert_eq!(recomputed.track_media_to_wall_clock_ratio_ppm, 1_000_000);
        assert_eq!(recomputed.track_tempo_window_count, 1);
        assert_eq!(recomputed.track_tempo_window_fast_count, 0);
        assert_eq!(recomputed.track_tempo_window_slow_count, 0);
    }

    #[test]
    fn raw_playback_recompute_uses_send_start_cadence_not_completion_jitter() {
        let metrics = PlaybackStabilitySnapshot {
            raw_send_events: (0..TRACK_TEMPO_WINDOW_PACKETS as u64)
                .map(|index| {
                    let send_started_offset_us = index * 20_000;
                    let sent_offset_us =
                        send_started_offset_us + if index % 2 == 0 { 200 } else { 100 };
                    discord_voice_service_twilight::PlaybackSendEventSnapshot {
                        packet_index: index,
                        command_kind: PlaybackSendCommandKind::Track,
                        expected_deadline_offset_us: send_started_offset_us,
                        send_started_offset_us,
                        sent_offset_us,
                        media_duration_ms: 20,
                        media_duration_samples: 960,
                        rtp_sequence: index as u32,
                        rtp_timestamp: (index * 960) as u32,
                        protection_nonce: Some(index as u32),
                        source_frame_epoch: Some(1),
                        source_media_position_ms: Some(index * 20),
                        source_media_position_samples: Some(index * 960),
                        source_media_byte_position: Some(index * 64),
                        committed_heard_media: true,
                    }
                })
                .collect(),
            ..Default::default()
        };

        let recomputed = recompute_raw_playback(&metrics, "completion jitter").unwrap();

        assert_eq!(recomputed.track_fast_interval_count, 0);
        assert_eq!(recomputed.track_fast_interval_min_us, 0);
        assert_eq!(recomputed.track_wall_clock_elapsed_ms, 1_000);
        assert_eq!(recomputed.track_media_to_wall_clock_ratio_ppm, 1_000_000);
        assert_eq!(recomputed.track_tempo_window_fast_count, 0);
        assert_eq!(recomputed.track_tempo_window_slow_count, 0);
    }

    #[test]
    fn playback_timing_budget_accepts_fractional_track_packets_through_raw_validation() {
        let (metrics, post_source_window_count) = fractional_playback_timing_snapshot();

        validate_playback_timing_budget(&metrics, "fractional playback").unwrap();
        assert_eq!(post_source_window_count, 501);
        assert!(
            metrics
                .raw_send_events
                .iter()
                .filter(|event| event.command_kind == PlaybackSendCommandKind::Track)
                .all(|event| event.media_duration_ms == 2
                    && event.media_duration_samples == FRACTIONAL_TRACK_DURATION_SAMPLES)
        );
        assert!(
            metrics
                .raw_prepared_playout_queue_events
                .iter()
                .filter(|event| event.command_kind == PlaybackSendCommandKind::Track)
                .all(|event| event.media_duration_ms == 2
                    && event.media_duration_samples == FRACTIONAL_TRACK_DURATION_SAMPLES)
        );
    }

    #[test]
    fn playback_timing_budget_rejects_fractional_track_packets_paced_at_floored_ms() {
        let (mut metrics, _) = fractional_playback_timing_snapshot();
        let mut next_offset_us = 0u64;
        for event in &mut metrics.raw_send_events {
            let sent_offset_us = next_offset_us;
            event.expected_deadline_offset_us = sent_offset_us;
            event.send_started_offset_us = sent_offset_us;
            event.sent_offset_us = sent_offset_us;
            next_offset_us = next_offset_us.saturating_add(match event.command_kind {
                PlaybackSendCommandKind::Track => 2_000,
                _ => event.media_duration_ms.saturating_mul(1_000),
            });
        }

        let error = validate_playback_timing_budget(&metrics, "fractional playback")
            .expect_err("raw validation should reject 120-sample packets paced every 2ms");
        assert!(
            error.to_string().contains("raw send events recomputed"),
            "unexpected error: {error}"
        );
    }

    fn fractional_playback_timing_snapshot() -> (PlaybackStabilitySnapshot, u64) {
        const TRACK_PACKETS: u64 = 2_550;

        let mut metrics = valid_playback_timing_snapshot();
        let track_media_samples = TRACK_PACKETS * u64::from(FRACTIONAL_TRACK_DURATION_SAMPLES);
        let track_media_ms = duration_ms_from_samples(track_media_samples);
        let tempo_window_count = segmented_track_tempo_window_count(
            TRACK_PACKETS,
            PAUSE_STOP_SILENCE_FRAME_COUNT as u64,
        );
        let post_source_window_count = segmented_post_source_tempo_window_count(
            TRACK_PACKETS,
            PAUSE_STOP_SILENCE_FRAME_COUNT as u64,
            FRACTIONAL_TRACK_DURATION_SAMPLES,
        );

        metrics.track_packet_count = TRACK_PACKETS;
        metrics.expected_track_frame_count = TRACK_PACKETS;
        metrics.sent_track_frame_count = TRACK_PACKETS;
        metrics.track_interval = duration_stats(TRACK_PACKETS - 1, 2, 3, 3, 2, 3);
        metrics.track_media_duration_sent_ms = track_media_ms;
        metrics.track_wall_clock_elapsed_ms = track_media_ms;
        metrics.track_media_to_wall_clock_ratio_ppm = 1_000_000;
        metrics.track_tempo_window_count = tempo_window_count;
        metrics.track_tempo_window_post_source_buffer_count = post_source_window_count;
        metrics.track_tempo_window_min_ratio_ppm = 1_000_000;
        metrics.track_tempo_window_max_ratio_ppm = 1_000_000;
        metrics.raw_send_events = raw_send_events_with_track_duration(
            TRACK_PACKETS,
            PAUSE_STOP_SILENCE_FRAME_COUNT as u64,
            FRACTIONAL_TRACK_DURATION_SAMPLES,
        );
        metrics.raw_prepared_playout_queue_events =
            raw_prepared_playout_queue_events_with_track_duration(
                TRACK_PACKETS,
                FRACTIONAL_TRACK_DURATION_SAMPLES,
            );

        (metrics, post_source_window_count)
    }

    #[test]
    fn live_runtime_metrics_require_five_hundred_post_source_windows() {
        let metrics = PlaybackStabilitySnapshot {
            track_tempo_window_post_source_buffer_count: MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS - 1,
            ..valid_playback_timing_snapshot()
        };

        let error = validate_live_runtime_post_source_window_count(&metrics, "finished playback")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("runtime post-source tempo windows"),
            "unexpected error: {error}"
        );

        let metrics = PlaybackStabilitySnapshot {
            track_tempo_window_post_source_buffer_count: MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS,
            ..valid_playback_timing_snapshot()
        };
        validate_live_runtime_post_source_window_count(&metrics, "finished playback").unwrap();
    }

    #[test]
    fn active_interrupt_state_checks_require_stopped_or_left_shape() {
        let stopped = StateSnapshot {
            state: SessionState::VoiceReady,
            guild_id: Some(Id::new(1)),
            channel_id: Some(Id::new(2)),
            current_video_id: None,
            queue_depth: 0,
            selected_itag: None,
            message: None,
        };
        ensure_state_after_active_stop(&stopped).unwrap();

        let still_playing = StateSnapshot {
            state: SessionState::Playing,
            ..stopped.clone()
        };
        assert!(ensure_state_after_active_stop(&still_playing).is_err());

        let left = StateSnapshot {
            state: SessionState::Idle,
            guild_id: None,
            channel_id: None,
            current_video_id: None,
            queue_depth: 0,
            selected_itag: None,
            message: None,
        };
        ensure_state_after_active_leave_voice(&left).unwrap();

        let still_joined = StateSnapshot {
            state: SessionState::Idle,
            guild_id: Some(Id::new(1)),
            ..left
        };
        assert!(ensure_state_after_active_leave_voice(&still_joined).is_err());
    }

    fn valid_test_config() -> StagingConfig {
        StagingConfig::from_env_map(HashMap::from([
            ("BOT_TOKEN".to_owned(), "token".to_owned()),
            ("OBSERVER_BOT_TOKEN".to_owned(), "observer-token".to_owned()),
            ("APPLICATION_ID".to_owned(), "1".to_owned()),
            ("TEST_GUILD_ID".to_owned(), "2".to_owned()),
            ("TEST_VOICE_CHANNEL_ID".to_owned(), "3".to_owned()),
            ("TEST_VIDEO_ID".to_owned(), "video".to_owned()),
            (
                "DISCORD_VOICE_SERVICE_URI".to_owned(),
                "http://127.0.0.1:55051".to_owned(),
            ),
            (
                "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR".to_owned(),
                "http://127.0.0.1:50051".to_owned(),
            ),
            (
                "LIVE_STAGING_PROFILE".to_owned(),
                "constrained-github-hosted".to_owned(),
            ),
            ("LIVE_STAGING_SERVICE_CPUS".to_owned(), "1.0".to_owned()),
            (
                "LIVE_STAGING_CPU_CONTENTION_WORKERS".to_owned(),
                "2".to_owned(),
            ),
            ("LIVE_STAGING_HTTP_READ_DELAY_MS".to_owned(), "5".to_owned()),
            (
                "LIVE_STAGING_HTTP_READ_JITTER_MS".to_owned(),
                "25".to_owned(),
            ),
        ]))
        .expect("config should parse")
    }

    #[tokio::test]
    async fn waits_for_voice_ready_before_continuing_live_contract() {
        let mut events = stream::iter(vec![
            Ok(event(SessionEventKind::VoiceConnecting, None)),
            Ok(event(SessionEventKind::VoiceReady, None)),
            Ok(event(SessionEventKind::TrackResolving, Some("video"))),
            Ok(event(SessionEventKind::Buffering, Some("video"))),
            Ok(event(SessionEventKind::Playing, Some("video"))),
            Ok(event(SessionEventKind::Paused, Some("video"))),
            Ok(event(SessionEventKind::Playing, Some("video"))),
            Ok(event(SessionEventKind::TrackEnded, Some("video"))),
        ]);

        let initial =
            wait_for_initial_voice_ready(&mut events, "video", LiveContractState::default())
                .await
                .expect("voice ready should be observed before play");
        assert!(initial.saw_voice_ready);
        assert!(!initial.saw_playing);

        let final_state = wait_for_play_completed_contract(&mut events, "video", initial)
            .await
            .expect("remaining events should satisfy the play-completion contract");
        assert!(final_state.saw_voice_ready);
        assert!(final_state.saw_buffering);
        assert!(final_state.saw_playing);
        assert!(final_state.saw_paused);
        assert!(final_state.saw_resumed_playing);
        assert!(final_state.saw_track_ended);
    }

    #[tokio::test]
    async fn post_play_contract_resets_pre_play_track_progress() {
        let initial = LiveContractState {
            saw_voice_ready: true,
            saw_track_resolving: true,
            saw_buffering: true,
            saw_playing: true,
            saw_paused: true,
            saw_resumed_playing: true,
            saw_track_ended: true,
            ..LiveContractState::default()
        };

        let post_play_state = post_play_live_contract_state(&initial);
        assert!(post_play_state.saw_voice_ready);
        assert!(!post_play_state.saw_track_resolving);
        assert!(!post_play_state.saw_buffering);
        assert!(!post_play_state.saw_playing);
        assert!(!post_play_state.saw_paused);
        assert!(!post_play_state.saw_resumed_playing);
        assert!(!post_play_state.saw_track_ended);

        let mut events = stream::iter(vec![
            Ok(event(SessionEventKind::TrackResolving, Some("video"))),
            Ok(event(SessionEventKind::Buffering, Some("video"))),
            Ok(event(SessionEventKind::Playing, Some("video"))),
            Ok(event(SessionEventKind::Paused, Some("video"))),
            Ok(event(SessionEventKind::Playing, Some("video"))),
            Ok(event(SessionEventKind::TrackEnded, Some("video"))),
        ]);

        let final_state = wait_for_play_completed_contract(&mut events, "video", post_play_state)
            .await
            .expect("a post-Play TrackEnded event should satisfy the play-completion contract");
        assert!(final_state.saw_voice_ready);
        assert!(final_state.saw_buffering);
        assert!(final_state.saw_playing);
        assert!(final_state.saw_paused);
        assert!(final_state.saw_resumed_playing);
        assert!(final_state.saw_track_ended);
    }

    #[tokio::test]
    async fn play_completion_is_required_after_contract_finishes() {
        let (play_tx, play_rx) = oneshot::channel::<()>();
        let contract = async {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok::<_, anyhow::Error>(LiveContractState {
                saw_voice_ready: true,
                saw_track_resolving: true,
                saw_playing: true,
                saw_track_ended: true,
                ..LiveContractState::default()
            })
        };
        let play = async {
            play_rx.await.expect("play completion should be signaled");
            Ok::<_, anyhow::Error>(())
        };

        let mut orchestration = Box::pin(wait_for_play_and_live_contract(contract, play));
        timeout(std::time::Duration::from_millis(25), &mut orchestration)
            .await
            .expect_err("validation must not finish before Play completes");

        play_tx.send(()).expect("play signal should send");
        let state = orchestration
            .await
            .expect("validation should finish after Play completes");
        assert!(state.saw_voice_ready);
        assert!(state.saw_playing);
        assert!(state.saw_track_ended);
    }

    #[test]
    fn live_test_video_duration_requires_at_least_ninety_seconds() {
        let error = validate_live_test_video_duration(89_999, "video").unwrap_err();
        assert!(
            error.to_string().contains("too short for live staging"),
            "unexpected error: {error}"
        );

        validate_live_test_video_duration(90_000, "video").unwrap();
    }

    #[test]
    fn observer_thresholds_require_audio_near_expected_duration() {
        let expected_duration_ms = 180_000;
        let required_decoded_audio_ms = required_observer_decoded_audio_ms(expected_duration_ms);
        assert_eq!(required_decoded_audio_ms, 178_000);

        let short_stats = AudioValidationStats {
            observed_packet_count: 700,
            decoded_audio_ms: 14_000,
            non_silent_audio_ms: 14_000,
            ..Default::default()
        };
        assert!(!observer_thresholds_satisfied(
            &short_stats,
            expected_duration_ms,
        ));

        let full_stats = AudioValidationStats {
            observed_packet_count: 8_100,
            decoded_audio_ms: required_decoded_audio_ms,
            wall_clock_elapsed_ms: required_decoded_audio_ms,
            decoded_audio_to_wall_clock_ratio_ppm: 1_000_000,
            non_silent_audio_ms: 120_000,
            decoded_audio_tempo_window_count: 8_051,
            decoded_audio_tempo_window_post_source_buffer_count: 7_801,
            decoded_audio_tempo_window_min_ratio_ppm: 1_000_000,
            decoded_audio_tempo_window_max_ratio_ppm: 1_000_000,
            decoded_audio_tempo_window_fast_count: 0,
            decoded_audio_tempo_window_slow_count: 0,
            decoded_audio_short_tempo_window_count: 16_100,
            decoded_audio_short_tempo_window_fast_count: 0,
            decoded_audio_short_tempo_window_slow_count: 0,
            decoded_audio_short_tempo_window_fastest: Some(observer_short_window(
                1_000_000, 500_000,
            )),
            decoded_audio_short_tempo_window_slowest: Some(observer_short_window(
                1_000_000, 500_000,
            )),
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 8_099,
                p50_ms: 20,
                p95_ms: 20,
                p99_ms: 20,
                min_ms: 20,
                max_ms: 20,
            },
            ..Default::default()
        };
        assert!(observer_thresholds_satisfied(
            &full_stats,
            expected_duration_ms,
        ));

        let receive_jitter_with_stable_decoded_tempo = AudioValidationStats {
            rtp_fast_interval_count: 2_147,
            rtp_fast_interval_min_ms: 5,
            rtp_fast_interval_min_us: 5_628,
            observer_anomalies: vec![crate::audio::AudioObserverAnomaly {
                kind: "rtp_fast_interval".to_owned(),
                classification: "post_resume_steady_playback".to_owned(),
                sequence: Some(2_735),
                previous_sequence: Some(2_734),
                interval_ms: 18,
                interval_us: 18_370,
                expected_duration_ms: 20,
                expected_duration_us: 20_000,
            }],
            decoded_audio_short_tempo_window_fast_count: 13,
            decoded_audio_short_tempo_window_slow_count: 109,
            decoded_audio_short_tempo_window_fastest: Some(
                crate::audio::AudioTempoWindowEvidence {
                    classification: "post_resume_steady_playback".to_owned(),
                    window_packet_count: 25,
                    media_ms: 500,
                    wall_clock_us: 483_283,
                    ratio_ppm: 1_034_590,
                    first_sequence: 2_735,
                    last_sequence: 2_759,
                },
            ),
            decoded_audio_short_tempo_window_slowest: Some(
                crate::audio::AudioTempoWindowEvidence {
                    classification: "post_resume_steady_playback".to_owned(),
                    window_packet_count: 25,
                    media_ms: 500,
                    wall_clock_us: 522_978,
                    ratio_ppm: 956_063,
                    first_sequence: 2_711,
                    last_sequence: 2_735,
                },
            ),
            decoded_audio_tempo_window_min_ratio_ppm: 991_751,
            decoded_audio_tempo_window_max_ratio_ppm: 999_029,
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 6_142,
                p50_ms: 20,
                p95_ms: 25,
                p99_ms: 29,
                min_ms: 5,
                max_ms: 36,
            },
            ..full_stats.clone()
        };
        assert!(
            observer_thresholds_satisfied(
                &receive_jitter_with_stable_decoded_tempo,
                expected_duration_ms,
            ),
            "receive-side RTP jitter within the short-window jitter budget should not fail live audio proof"
        );

        let micro_receive_jitter_with_stable_decoded_tempo = AudioValidationStats {
            decoded_audio_short_tempo_window_fast_count: 13,
            decoded_audio_short_tempo_window_slow_count: 16,
            decoded_audio_short_tempo_window_fastest: Some(observer_short_window(
                1_107_957, 451_281,
            )),
            decoded_audio_short_tempo_window_slowest: Some(observer_short_window(924_616, 540_765)),
            decoded_audio_tempo_window_min_ratio_ppm: 990_213,
            decoded_audio_tempo_window_max_ratio_ppm: 1_009_161,
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 6_141,
                p50_ms: 20,
                p95_ms: 24,
                p99_ms: 27,
                min_ms: 2,
                max_ms: 39,
            },
            ..full_stats.clone()
        };
        assert!(
            observer_thresholds_satisfied(
                &micro_receive_jitter_with_stable_decoded_tempo,
                expected_duration_ms,
            ),
            "25-packet receive-side jitter should not fail when aggregate and long-window decoded tempo remain stable"
        );

        let speed_change_budget_us =
            observer_speed_change_total_abs_budget_us(expected_duration_ms);
        assert_eq!(speed_change_budget_us, 900_000);

        let cumulative_receive_speed_change = AudioValidationStats {
            rtp_speed_change_total_abs_us: speed_change_budget_us + 1,
            rtp_speed_change_total_fast_us: speed_change_budget_us + 1,
            decoded_audio_to_wall_clock_ratio_ppm: 1_000_000,
            decoded_audio_tempo_window_fast_count: 0,
            decoded_audio_tempo_window_slow_count: 0,
            decoded_audio_tempo_window_min_ratio_ppm: 1_000_000,
            decoded_audio_tempo_window_max_ratio_ppm: 1_000_000,
            ..full_stats.clone()
        };
        assert!(
            !observer_thresholds_satisfied(&cumulative_receive_speed_change, expected_duration_ms),
            "cumulative receive-side speed-up must fail even when aggregate tempo windows still look stable"
        );

        let fast_playback_stats = AudioValidationStats {
            wall_clock_elapsed_ms: required_decoded_audio_ms.saturating_sub(20_000),
            decoded_audio_to_wall_clock_ratio_ppm: MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM + 1,
            rtp_fast_interval_count: 1,
            rtp_fast_interval_min_ms: 8,
            rtp_fast_interval_min_us: 8_000,
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &fast_playback_stats,
            expected_duration_ms,
        ));

        let isolated_receive_fast_interval_with_stable_ratio = AudioValidationStats {
            rtp_fast_interval_count: 1,
            rtp_fast_interval_min_ms: 19,
            rtp_fast_interval_min_us: 19_000,
            decoded_audio_to_wall_clock_ratio_ppm: 1_000_000,
            decoded_audio_tempo_window_fast_count: 0,
            decoded_audio_tempo_window_slow_count: 0,
            decoded_audio_tempo_window_min_ratio_ppm: 1_000_000,
            decoded_audio_tempo_window_max_ratio_ppm: 1_000_000,
            ..full_stats.clone()
        };
        assert!(observer_thresholds_satisfied(
            &isolated_receive_fast_interval_with_stable_ratio,
            expected_duration_ms,
        ));

        let materially_fast_short_window = AudioValidationStats {
            decoded_audio_short_tempo_window_fast_count: 1,
            decoded_audio_short_tempo_window_fastest: Some(observer_short_window_with_packets(
                OBSERVER_SHORT_WINDOW_MAX_RATIO_PPM + 1,
                943_396,
                OBSERVER_STRICT_SHORT_WINDOW_MIN_PACKETS,
            )),
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &materially_fast_short_window,
            expected_duration_ms,
        ));

        let materially_slow_short_window = AudioValidationStats {
            decoded_audio_short_tempo_window_slow_count: 1,
            decoded_audio_short_tempo_window_slowest: Some(observer_short_window_with_packets(
                OBSERVER_SHORT_WINDOW_MIN_RATIO_PPM - 1,
                1_063_831,
                OBSERVER_STRICT_SHORT_WINDOW_MIN_PACKETS,
            )),
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &materially_slow_short_window,
            expected_duration_ms,
        ));

        let slow_playback_stats = AudioValidationStats {
            wall_clock_elapsed_ms: required_decoded_audio_ms.saturating_add(20_000),
            decoded_audio_to_wall_clock_ratio_ppm: MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM - 1,
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &slow_playback_stats,
            expected_duration_ms,
        ));

        let isolated_observer_gap_stats = AudioValidationStats {
            rtp_gap_count_gte_100ms: 1,
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 8_099,
                p50_ms: 20,
                p95_ms: 26,
                p99_ms: 31,
                min_ms: 1,
                max_ms: 104,
            },
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &isolated_observer_gap_stats,
            expected_duration_ms,
        ));

        let controlled_pause_gap_stats = AudioValidationStats {
            rtp_gap_count_gte_100ms: 1,
            observer_anomalies: vec![crate::audio::AudioObserverAnomaly {
                kind: "rtp_gap_gte_100ms".to_owned(),
                classification: "controlled_pause".to_owned(),
                sequence: Some(10),
                previous_sequence: Some(9),
                interval_ms: 3_000,
                interval_us: 3_000_000,
                expected_duration_ms: 20,
                expected_duration_us: 20_000,
            }],
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 8_099,
                p50_ms: 20,
                p95_ms: 26,
                p99_ms: 31,
                min_ms: 1,
                max_ms: 104,
            },
            ..full_stats.clone()
        };
        assert!(observer_thresholds_satisfied(
            &controlled_pause_gap_stats,
            expected_duration_ms,
        ));

        let observer_buffering_stats = AudioValidationStats {
            rtp_buffering_event_count: 1,
            rtp_buffering_total_us: 80_000,
            rtp_buffering_max_us: 80_000,
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 8_099,
                p50_ms: 20,
                p95_ms: 20,
                p99_ms: 20,
                min_ms: 20,
                max_ms: 20,
            },
            ..full_stats.clone()
        };
        assert!(
            !observer_thresholds_satisfied(&observer_buffering_stats, expected_duration_ms),
            "any steady-playback buffering observed in received RTP must fail live audio proof"
        );

        let sustained_gapped_stats = AudioValidationStats {
            rtp_gap_count_gte_100ms: 10,
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 8_099,
                p50_ms: 20,
                p95_ms: RTP_INTERVAL_P95_BUDGET_MS + 1,
                p99_ms: RTP_INTERVAL_P99_BUDGET_MS + 1,
                min_ms: 1,
                max_ms: RTP_INTERVAL_MAX_BUDGET_MS + 1,
            },
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &sustained_gapped_stats,
            expected_duration_ms,
        ));

        let missing_post_reservoir_window_stats = AudioValidationStats {
            decoded_audio_tempo_window_post_source_buffer_count: 0,
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &missing_post_reservoir_window_stats,
            expected_duration_ms,
        ));

        let too_few_post_reservoir_window_stats = AudioValidationStats {
            decoded_audio_tempo_window_post_source_buffer_count: MIN_LIVE_POST_SOURCE_TEMPO_WINDOWS
                - 1,
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &too_few_post_reservoir_window_stats,
            expected_duration_ms,
        ));

        let fast_rolling_window_stats = AudioValidationStats {
            decoded_audio_tempo_window_fast_count: 1,
            decoded_audio_tempo_window_fastest_ratio_ppm: MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM + 1,
            decoded_audio_tempo_window_fastest_media_ms: 1_000,
            decoded_audio_tempo_window_fastest_wall_clock_us: 980_000,
            decoded_audio_tempo_window_max_ratio_ppm: MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM + 1,
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &fast_rolling_window_stats,
            expected_duration_ms,
        ));

        let slow_rolling_window_stats = AudioValidationStats {
            decoded_audio_tempo_window_slow_count: 1,
            decoded_audio_tempo_window_slowest_ratio_ppm: MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM - 1,
            decoded_audio_tempo_window_slowest_media_ms: 1_000,
            decoded_audio_tempo_window_slowest_wall_clock_us: 1_030_000,
            decoded_audio_tempo_window_min_ratio_ppm: MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM - 1,
            ..full_stats.clone()
        };
        assert!(!observer_thresholds_satisfied(
            &slow_rolling_window_stats,
            expected_duration_ms,
        ));

        let high_p99_stats = AudioValidationStats {
            rtp_inter_arrival: crate::audio::AudioIntervalStats {
                samples: 8_099,
                p50_ms: 20,
                p95_ms: 40,
                p99_ms: RTP_INTERVAL_P99_BUDGET_MS + 1,
                min_ms: 18,
                max_ms: RTP_INTERVAL_P99_BUDGET_MS + 1,
            },
            ..full_stats
        };
        assert!(!observer_thresholds_satisfied(
            &high_p99_stats,
            expected_duration_ms,
        ));
    }

    #[tokio::test]
    async fn pause_proof_requires_silence_after_observed_self_mute() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let error = prove_observer_pause_silence(
            &mut session,
            "speaker-1",
            &mut accumulator,
            &snapshot,
            &mut audio_started,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("observer timed out waiting for service speaking disappearance")
        );
        assert_eq!(accumulator.stats().observed_packet_count, 0);
    }

    #[tokio::test]
    async fn pause_proof_reports_gateway_speaking_stop_without_media_boundary() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let proof = async {
            prove_observer_pause_silence(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
            )
            .await
        };
        let send_stop = async {
            fake.send_speaking_state_without_user(0, 42).await.unwrap();
        };
        let (proof, ()) = tokio::join!(proof, send_stop);
        let proof = proof.unwrap();

        assert!(proof.gateway_speaking_stopped);
        assert!(!proof.rtp_silence_observed);
        assert_eq!(proof.silence_ms, 600);
        assert_eq!(accumulator.stats().observed_packet_count, 0);
    }

    #[tokio::test]
    async fn pause_proof_does_not_treat_single_rtp_silence_as_boundary() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let proof = async {
            prove_observer_pause_silence(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
            )
            .await
        };
        let send_audio_then_silence = async {
            let mode = fake.encryption_mode().await.unwrap();
            let secret_key = fake.secret_key().await.unwrap();
            let protection = ProtectionContext::new(mode, secret_key).unwrap();
            let rtp = RtpPacketBuilder::new(42);
            let header = rtp.build_header(1, 960);
            let packet = protection
                .protect_packet(&header, &OPUS_SILENCE_FRAME)
                .unwrap();
            fake.send_raw_udp_packet(&packet).await.unwrap();
        };
        let (proof, ()) = tokio::join!(proof, send_audio_then_silence);
        let proof = proof.unwrap();

        assert!(!proof.gateway_speaking_stopped);
        assert!(!proof.rtp_silence_observed);
        assert_eq!(proof.silence_ms, 600);
        assert_eq!(accumulator.stats().observed_packet_count, 1);
    }

    #[tokio::test]
    async fn pause_proof_accepts_explicit_stop_silence_tail_without_gateway_speaking_stop() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let proof = async {
            prove_observer_pause_silence(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
            )
            .await
        };
        let send_stop_tail = async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mode = fake.encryption_mode().await.unwrap();
            let secret_key = fake.secret_key().await.unwrap();
            let protection = ProtectionContext::new(mode, secret_key).unwrap();
            let rtp = RtpPacketBuilder::new(42);
            for sequence in 0..PAUSE_STOP_SILENCE_FRAME_COUNT {
                let sequence = u16::try_from(sequence).unwrap();
                let header = rtp.build_header(sequence, u32::from(sequence) * 960);
                let packet = protection
                    .protect_packet(&header, &OPUS_SILENCE_FRAME)
                    .unwrap();
                fake.send_raw_udp_packet(&packet).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let (proof, ()) = tokio::join!(proof, send_stop_tail);
        let proof = proof.unwrap();

        assert!(!proof.gateway_speaking_stopped);
        assert!(proof.rtp_silence_observed);
        assert_eq!(proof.silence_ms, 600);
        assert_eq!(accumulator.stats().observed_packet_count, 5);
    }

    #[tokio::test]
    async fn pause_proof_rejects_voice_disconnect_during_silence() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let proof = async {
            prove_observer_pause_silence(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
            )
            .await
        };
        let send_disconnect = async {
            fake.send_client_disconnect("speaker-1").await.unwrap();
        };
        let (error, ()) = tokio::join!(proof, send_disconnect);
        let error = error.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("observer saw service voice client disconnect during Pause proof")
        );
        assert_eq!(accumulator.stats().observed_packet_count, 0);
    }

    #[tokio::test]
    async fn resume_proof_requires_observed_speaking_start_and_audio_packets() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let proof = async {
            prove_observer_resume_audio(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
                false,
            )
            .await
        };
        let send_resume = async {
            fake.send_speaking("speaker-1", 42).await.unwrap();
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mode = fake.encryption_mode().await.unwrap();
            let secret_key = fake.secret_key().await.unwrap();
            let protection = ProtectionContext::new(mode, secret_key).unwrap();
            let rtp = RtpPacketBuilder::new(42);
            for sequence in 0..(RESUME_OBSERVER_PACKET_TARGET * 4) {
                let sequence = u16::try_from(sequence).unwrap();
                let header = rtp.build_header(sequence, u32::from(sequence) * 960);
                let packet = protection
                    .protect_packet(&header, &OPUS_SILENCE_FRAME)
                    .unwrap();
                fake.send_raw_udp_packet(&packet).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let (proof, ()) = tokio::join!(proof, send_resume);
        let proof = proof.unwrap();

        assert!(proof.speaking_started);
        assert_eq!(proof.observed_packet_count, RESUME_OBSERVER_PACKET_TARGET);
    }

    #[tokio::test]
    async fn resume_proof_rejects_resumed_audio_without_speaking_start() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();
        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let proof = async {
            prove_observer_resume_audio(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
                false,
            )
            .await
        };
        let send_resume_audio = async {
            let mode = fake.encryption_mode().await.unwrap();
            let secret_key = fake.secret_key().await.unwrap();
            let protection = ProtectionContext::new(mode, secret_key).unwrap();
            let rtp = RtpPacketBuilder::new(42);
            for sequence in 0..RESUME_OBSERVER_PACKET_TARGET {
                let sequence = u16::try_from(sequence).unwrap();
                let header = rtp.build_header(sequence, u32::from(sequence) * 960);
                let packet = protection
                    .protect_packet(&header, &OPUS_SILENCE_FRAME)
                    .unwrap();
                fake.send_raw_udp_packet(&packet).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let (proof, ()) = tokio::join!(proof, send_resume_audio);
        let error = proof.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("observer received resumed service audio before service Speaking 1")
        );
    }

    #[tokio::test]
    async fn resume_proof_accepts_resumed_audio_after_explicit_pause_boundary_without_gateway_echo()
    {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let observed_at = std::time::Instant::now();
        for sequence in 0..PAUSE_STOP_SILENCE_FRAME_COUNT {
            let sequence = u16::try_from(sequence).unwrap();
            accumulator
                .observe_packet_at(
                    ObservedOpusPacket {
                        sequence,
                        timestamp: u32::from(sequence) * 960,
                        payload: &OPUS_STOP_SILENCE_FRAME,
                    },
                    observed_at + Duration::from_millis(u64::from(sequence) * 20),
                )
                .unwrap();
        }
        accumulator.reset_wall_clock_baseline_after_controlled_pause();

        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let proof = async {
            prove_observer_resume_audio(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
                true,
            )
            .await
        };
        let send_resume_audio = async {
            tokio::time::sleep(Duration::from_millis(125)).await;
            let mode = fake.encryption_mode().await.unwrap();
            let secret_key = fake.secret_key().await.unwrap();
            let protection = ProtectionContext::new(mode, secret_key).unwrap();
            let rtp = RtpPacketBuilder::new(42);
            for sequence in PAUSE_STOP_SILENCE_FRAME_COUNT
                ..(PAUSE_STOP_SILENCE_FRAME_COUNT + RESUME_OBSERVER_PACKET_TARGET as usize)
            {
                let sequence = u16::try_from(sequence).unwrap();
                let header = rtp.build_header(sequence, u32::from(sequence) * 960);
                let packet = protection
                    .protect_packet(&header, &OPUS_SILENCE_FRAME)
                    .unwrap();
                fake.send_raw_udp_packet(&packet).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let (proof, ()) = tokio::join!(proof, send_resume_audio);
        let proof = proof.unwrap();

        assert!(!proof.speaking_started);
        assert_eq!(proof.observed_packet_count, RESUME_OBSERVER_PACKET_TARGET);
        assert_eq!(accumulator.stats().rtp_gap_count_gte_100ms, 0);
    }

    #[tokio::test]
    async fn pause_proof_request_waits_until_observer_is_armed() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut request = tokio::spawn(async move { start_observer_pause_proof(&tx).await });

        let Some(ObserverAudioProofCommand::Pause {
            armed,
            mut begin,
            respond_to,
        }) = rx.recv().await
        else {
            panic!("expected Pause proof request");
        };

        assert!(
            timeout(Duration::from_millis(50), &mut request)
                .await
                .is_err(),
            "Pause proof request must wait until the observer task is armed"
        );

        armed.send(()).unwrap();
        let mut proof = request.await.unwrap().unwrap();
        assert!(
            timeout(Duration::from_millis(50), &mut begin)
                .await
                .is_err(),
            "Pause proof request must not begin when it is only armed"
        );
        begin_observer_pause_proof(&mut proof).unwrap();
        begin.await.unwrap();
        respond_to
            .send(Ok(ObserverPauseProof {
                silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                gateway_speaking_stopped: true,
                rtp_silence_observed: true,
            }))
            .unwrap();
        let proof = await_observer_pause_proof(proof).await.unwrap();

        assert!(proof.gateway_speaking_stopped);
        assert!(proof.rtp_silence_observed);
        assert_eq!(
            proof.silence_ms,
            duration_ms(PAUSE_OBSERVER_SILENCE_DURATION)
        );
    }

    #[tokio::test]
    async fn pause_proof_arm_window_records_pre_pause_audio_before_begin() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;
        let (begin, begin_rx) = oneshot::channel();

        let proof_begin = async {
            wait_for_observer_pause_proof_begin(
                &mut session,
                "speaker-1",
                &mut accumulator,
                &snapshot,
                &mut audio_started,
                begin_rx,
            )
            .await
        };
        let send_pre_pause_audio = async {
            let mode = fake.encryption_mode().await.unwrap();
            let secret_key = fake.secret_key().await.unwrap();
            let protection = ProtectionContext::new(mode, secret_key).unwrap();
            let rtp = RtpPacketBuilder::new(42);
            for sequence in 0..2u16 {
                let header = rtp.build_header(sequence, u32::from(sequence) * 960);
                let packet = protection
                    .protect_packet(&header, &OPUS_SILENCE_FRAME)
                    .unwrap();
                fake.send_raw_udp_packet(&packet).await.unwrap();
                tokio::time::sleep(Duration::from_millis(120)).await;
            }
            begin.send(()).unwrap();
        };
        let (arm_result, ()) = tokio::join!(proof_begin, send_pre_pause_audio);
        let stats = arm_result.unwrap().unwrap();

        assert_eq!(stats.rtp_gap_count_gte_100ms, 1);
        assert_eq!(stats.observer_anomalies.len(), 1);
        assert_eq!(stats.observer_anomalies[0].kind, "rtp_gap_gte_100ms");
        assert_eq!(
            stats.observer_anomalies[0].classification,
            "pre_pause_steady_playback"
        );
    }

    #[tokio::test]
    async fn observer_fake_discord_records_stall_then_catch_up_as_pre_pause_anomalies() {
        let fake = FakeDiscordPeer::spawn_real_shape().await;
        let voice = fake.voice_context("1", "2", "observer-1", "session-1", "token-1");
        let mut session = ObservedVoiceSession::connect(voice).await.unwrap();
        fake.send_speaking("speaker-1", 42).await.unwrap();
        session
            .receive_speaking_state_from("speaker-1", 1, Duration::from_secs(1))
            .await
            .unwrap();

        let mut accumulator = AudioValidationAccumulator::new();
        let snapshot = Arc::new(Mutex::new(None));
        let mut audio_started = None;

        let receive_observer_audio = async {
            for _ in 0..3 {
                let frame = session
                    .receive_audio_frame_from("speaker-1", Duration::from_secs(2))
                    .await
                    .unwrap();
                record_observer_audio_frame(frame, &mut accumulator, &snapshot, &mut audio_started)
                    .unwrap();
            }
            accumulator.stats()
        };
        let send_stall_then_catch_up = async {
            let mode = fake.encryption_mode().await.unwrap();
            let secret_key = fake.secret_key().await.unwrap();
            let protection = ProtectionContext::new(mode, secret_key).unwrap();
            let rtp = RtpPacketBuilder::new(42);
            for sequence in 0..3u16 {
                if sequence == 1 {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }
                let header = rtp.build_header(sequence, u32::from(sequence) * 960);
                let packet = protection
                    .protect_packet(&header, &OPUS_SILENCE_FRAME)
                    .unwrap();
                fake.send_raw_udp_packet(&packet).await.unwrap();
            }
        };

        let (stats, ()) = tokio::join!(receive_observer_audio, send_stall_then_catch_up);

        assert_eq!(stats.rtp_gap_count_gte_100ms, 1);
        assert_eq!(stats.rtp_buffering_event_count, 1);
        assert!(stats.rtp_buffering_total_us > 0);
        assert!(stats.rtp_speed_change_total_abs_us > 0);
        assert_eq!(stats.rtp_fast_interval_count, 1);
        assert!(stats.rtp_fast_interval_min_us < 20_000);
        let anomaly_kinds = stats
            .observer_anomalies
            .iter()
            .map(|anomaly| anomaly.kind.as_str())
            .collect::<Vec<_>>();
        assert_eq!(anomaly_kinds, ["rtp_gap_gte_100ms", "rtp_fast_interval"]);
        assert!(
            stats
                .observer_anomalies
                .iter()
                .all(|anomaly| anomaly.classification == "pre_pause_steady_playback"),
            "fake Discord stall/catch-up anomalies should remain steady-playback evidence: {stats:?}"
        );
    }

    #[tokio::test]
    async fn resume_proof_request_waits_until_observer_is_armed() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut request = tokio::spawn(async move { start_observer_resume_proof(&tx).await });

        let Some(ObserverAudioProofCommand::Resume { armed, respond_to }) = rx.recv().await else {
            panic!("expected Resume proof request");
        };

        assert!(
            timeout(Duration::from_millis(50), &mut request)
                .await
                .is_err(),
            "Resume proof request must wait until the observer task is armed"
        );

        armed.send(()).unwrap();
        let response = request.await.unwrap().unwrap();
        respond_to
            .send(Ok(ObserverResumeProof {
                observed_packet_count: RESUME_OBSERVER_PACKET_TARGET,
                speaking_started: true,
                resume_decoded_audio_start_ms: 0,
            }))
            .unwrap();
        let proof = response.await.unwrap().unwrap();

        assert!(proof.speaking_started);
        assert_eq!(proof.observed_packet_count, RESUME_OBSERVER_PACKET_TARGET);
    }

    #[test]
    fn failure_evidence_preserves_live_contract_snapshot_fields() {
        let snapshot = FailureEvidenceSnapshot {
            live_contract: Some(LiveContractState {
                saw_voice_ready: true,
                saw_track_resolving: true,
                saw_buffering: true,
                saw_playing: true,
                saw_paused: true,
                saw_resumed_playing: true,
                saw_track_ended: true,
                ..LiveContractState::default()
            }),
            audio_stats: None,
            observer_playback: Some(ObserverPlaybackProof {
                pause_silence_ms: 600,
                pause_self_mute_observed: false,
                pause_speaking_stopped: true,
                pause_rtp_silence_observed: true,
                resume_speaking_started: false,
                resume_observed_packet_count: 0,
                resume_decoded_audio_start_ms: 0,
            }),
            playback_metrics: None,
            reconnect_probe_metrics: None,
        };

        let evidence = build_failure_evidence(
            &valid_test_config(),
            &anyhow!("observer audio proof timed out"),
            Some(&snapshot),
        );

        assert!(evidence.saw_voice_ready);
        assert!(evidence.saw_buffering);
        assert!(evidence.saw_playing);
        assert!(evidence.saw_paused);
        assert!(evidence.saw_resumed_playing);
        assert!(evidence.saw_track_ended);
        assert!(!evidence.observer_pause_self_mute_observed);
        assert!(evidence.observer_pause_speaking_stopped);
        assert!(evidence.observer_pause_rtp_silence_observed);
        assert!(!evidence.observer_resume_speaking_started);
        assert_eq!(evidence.observer_pause_silence_ms, 600);
        assert_eq!(evidence.observer_resume_packet_count, 0);
    }

    #[test]
    fn failure_evidence_preserves_partial_post_play_metrics() {
        let playback_metrics = PlaybackStabilityEvidence {
            video_id: Some("natural-track".to_owned()),
            track_packet_count: 144,
            ended: true,
            ..PlaybackStabilityEvidence::default()
        };
        let reconnect_probe_metrics = PlaybackStabilityEvidence {
            video_id: Some("natural-track".to_owned()),
            track_packet_count: 64,
            reconnect_interruptions: 1,
            ..PlaybackStabilityEvidence::default()
        };
        let mut snapshot = FailureEvidenceSnapshot::default();
        snapshot.record_post_play_evidence(PostPlayControlEvidence {
            playback_metrics: Some(playback_metrics.clone()),
            reconnect_probe_metrics: Some(reconnect_probe_metrics.clone()),
        });

        let evidence = build_failure_evidence(
            &valid_test_config(),
            &anyhow!("interrupted playback probe Play RPC failed before Playing"),
            Some(&snapshot),
        );

        assert_eq!(evidence.playback_metrics, Some(playback_metrics));
        assert_eq!(
            evidence.reconnect_probe_metrics,
            Some(reconnect_probe_metrics)
        );
    }

    #[test]
    fn failure_evidence_classifies_sender_source_skip() {
        let evidence = build_failure_evidence(
            &valid_test_config(),
            &anyhow!(
                "finished playback sender_source_skipped_ahead: raw source position jumped forward"
            ),
            None,
        );

        assert_eq!(
            evidence.failure_reason.as_deref(),
            Some("sender_source_skipped_ahead")
        );
    }

    #[test]
    fn success_evidence_reports_playback_dave_transition_counter() {
        let playback_metrics = PlaybackStabilityEvidence {
            dave_transition_count_during_playback: 3,
            ..PlaybackStabilityEvidence::default()
        };
        let outcome = ValidatedLiveOutcome {
            live_contract: LiveContractState::default(),
            audio_stats: AudioValidationStats::default(),
            observer_playback: ObserverPlaybackProof::default(),
            expected_duration_ms: 180_000,
            playback_metrics: Some(playback_metrics),
            reconnect_probe_metrics: None,
        };

        let evidence = build_success_evidence(&valid_test_config(), &outcome);

        assert_eq!(evidence.dave_transition_count_during_playback, 3);
    }

    #[tokio::test]
    async fn failure_evidence_keeps_play_progress_from_snapshot_updates() {
        let initial = LiveContractState {
            saw_voice_ready: true,
            saw_track_resolving: true,
            saw_buffering: true,
            saw_playing: false,
            saw_track_ended: false,
            ..LiveContractState::default()
        };
        let snapshot = Arc::new(Mutex::new(initial.clone()));
        let mut events = stream::iter(vec![
            Ok(event(SessionEventKind::Playing, Some("video"))),
            Err(tonic::Status::internal("event stream failed after Playing")),
        ]);

        let error = wait_for_play_completed_contract_with_snapshot(
            &mut events,
            "video",
            initial,
            Some(Arc::clone(&snapshot)),
        )
        .await
        .expect_err("event stream failure should prevent completion");
        assert!(error.to_string().contains("event stream failed"));
        let failure_snapshot = FailureEvidenceSnapshot {
            live_contract: Some(snapshot.lock().unwrap().clone()),
            audio_stats: None,
            observer_playback: None,
            playback_metrics: None,
            reconnect_probe_metrics: None,
        };
        let error = anyhow!("observer audio proof timed out");

        let evidence =
            build_failure_evidence(&valid_test_config(), &error, Some(&failure_snapshot));

        assert!(evidence.saw_voice_ready);
        assert!(evidence.saw_playing);
        assert!(!evidence.saw_track_ended);
    }

    #[tokio::test]
    async fn observer_dave_readiness_returns_ready_session_value() {
        let ready = await_observer_dave_ready(async { Ok::<_, anyhow::Error>(41usize) })
            .await
            .expect("ready observer session should pass through");

        assert_eq!(ready, 41);
    }

    #[tokio::test]
    async fn observer_dave_readiness_adds_context_on_failure() {
        let error =
            await_observer_dave_ready(async { Err::<(), _>(anyhow!("dave handshake failed")) })
                .await
                .expect_err("readiness failure should surface with context");

        let message = format!("{error:#}");
        assert!(message.contains("await observer dave readiness"));
        assert!(message.contains("dave handshake failed"));
    }
}
