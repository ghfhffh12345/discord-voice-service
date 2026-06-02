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
use twilight_gateway::{
    Event, EventTypeFlags, Intents, MessageSender, Shard, ShardId, StreamExt as _,
};
use twilight_http::{Client as HttpClient, error::ErrorType as HttpErrorType};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, UserMarker},
};
use twilight_model::voice::VoiceState;

use discord_voice_service_playback::YtMusicClient;
use discord_voice_service_twilight::{
    Client as VoiceServiceClient, SessionEvent, SessionEventKind, SessionState,
    VoiceContext as ServiceVoiceContext, VoiceContextTracker, join_voice_channel,
    leave_voice_channel,
};
use discord_voice_service_voice::{
    ObservedAudioFrame, ObservedVoiceActivity, ObservedVoiceSession, PendingObservedVoiceSession,
    VoiceContext as ObserverVoiceContext, VoiceError,
};

use crate::audio::{AudioValidationAccumulator, AudioValidationStats, ObservedOpusPacket};
use crate::config::StagingConfig;
use crate::contract::{
    LiveContractState, LiveValidationEvidence, emit_validation_evidence, finalize_success_evidence,
};

const AUTHENTIC_VOICE_EVENT_TIMEOUT: Duration = Duration::from_secs(45);
const GATEWAY_LEAVE_TIMEOUT: Duration = Duration::from_secs(30);
const GATEWAY_LEAVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_CONTRACT_TIMEOUT: Duration = Duration::from_secs(240);
const MIN_OBSERVED_PACKET_COUNT: u64 = 120;
const MIN_DECODED_AUDIO_MS: u64 = 3_000;
const MIN_NON_SILENT_AUDIO_MS: u64 = 1_000;
const OBSERVER_AUDIO_DURATION_TOLERANCE_MS: u64 = 2_000;
const OBSERVER_AUDIO_DURATION_MIN_RATIO_PERCENT: u64 = 90;
const OBSERVER_AUDIO_STARTED_TIMEOUT: Duration = Duration::from_secs(30);
const PAUSE_OBSERVER_SILENCE_DURATION: Duration = Duration::from_millis(600);
const PRE_PAUSE_SPEAKING_SETTLE_DURATION: Duration = Duration::from_millis(1_500);
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
}

#[derive(Debug, Clone, Default)]
struct ObserverPlaybackProof {
    pause_silence_ms: u64,
    pause_self_mute_observed: bool,
    pause_speaking_stopped: bool,
    resume_speaking_started: bool,
    resume_observed_packet_count: u64,
}

#[derive(Debug, Clone, Default)]
struct FailureEvidenceSnapshot {
    live_contract: Option<LiveContractState>,
    audio_stats: Option<AudioValidationStats>,
    observer_playback: Option<ObserverPlaybackProof>,
}

#[derive(Debug)]
struct ServiceFlowOutcome {
    result: Result<ValidatedLiveOutcome>,
    failure_snapshot: FailureEvidenceSnapshot,
    service_joined: bool,
}

#[derive(Debug)]
struct ObserverPauseProof {
    silence_ms: u64,
    speaking_stopped: bool,
}

#[derive(Debug)]
struct ObserverResumeProof {
    observed_packet_count: u64,
    speaking_started: bool,
}

enum ObserverAudioProofCommand {
    ProveSpeakingStarted {
        respond_to: oneshot::Sender<Result<()>>,
    },
    ProvePause {
        respond_to: oneshot::Sender<Result<ObserverPauseProof>>,
    },
    ProveResume {
        armed: oneshot::Sender<()>,
        respond_to: oneshot::Sender<Result<ObserverResumeProof>>,
    },
}

enum LiveGatewayDriverCommand {
    WaitForServiceVoiceContext {
        respond_to: oneshot::Sender<Result<ServiceVoiceContext>>,
    },
}

struct PendingServiceVoiceContextProof {
    respond_to: oneshot::Sender<Result<ServiceVoiceContext>>,
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
    state.saw_playing = false;
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
    if duration_ms == 0 {
        bail!(
            "ytmusic-service returned zero expected duration for TEST_VIDEO_ID={}",
            config.test_video_id
        );
    }

    info!(
        selected_itag = source.selected_itag,
        expected_duration_ms = duration_ms,
        "resolved expected playback duration for live validation",
    );

    Ok(duration_ms)
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
    let (gateway_driver_tx, gateway_driver_rx) = mpsc::channel(2);
    let service_gateway_sender = service_shard.sender();
    let service_voice_for_controls = forwarded_voice.clone();
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
                    gateway_driver_tx,
                    service_gateway_sender,
                    service_voice_for_controls,
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
        })
    };

    let validation_and_play = wait_for_play_and_live_contract(combined_validation, play_rpc);
    let live_gateway_config = LiveGatewayDriverConfig {
        service_addr: config.discord_voice_service_uri.clone(),
        initial_service_voice: forwarded_voice,
        guild_id: observer_join.guild_id,
        observer_user_id: observer_join.user_id,
    };
    let live_gateway_driver = drive_live_gateway_shards(
        service_shard,
        observer_shard,
        live_gateway_config,
        gateway_driver_rx,
    );
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
    if let Ok(validated) = &mut result {
        validated.live_contract.mark_play();
        update_live_contract_snapshot(&live_contract_snapshot, &validated.live_contract);

        if let Err(error) = validate_post_play_control_rpcs(
            &mut control_client,
            &mut validated.live_contract,
            &live_contract_snapshot,
        )
        .await
        {
            result = Err(error);
        } else {
            service_joined = false;
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
    mut commands: mpsc::Receiver<LiveGatewayDriverCommand>,
) -> Result<()> {
    let mut update_client = VoiceServiceClient::connect(config.service_addr)
        .await
        .context("connect Twilight adapter client for live gateway driver")?;
    let mut service_voice = VoiceContextTracker::from_context(config.initial_service_voice);
    let mut pending_voice_context_proof = None::<PendingServiceVoiceContextProof>;

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    continue;
                };
                match command {
                    LiveGatewayDriverCommand::WaitForServiceVoiceContext { respond_to } => {
                        if pending_voice_context_proof.is_some() {
                            let _ = respond_to.send(Err(anyhow!(
                                "service voice context proof already pending"
                            )));
                            continue;
                        }

                        pending_voice_context_proof = Some(PendingServiceVoiceContextProof {
                            respond_to,
                        });
                    }
                }
            }
            item = service_shard.next_event(EventTypeFlags::all()) => {
                let Some(event) = next_live_gateway_event("service", item)? else {
                    continue;
                };
                if let Some(context) = service_voice.observe(&event) {
                    info!("forwarding refreshed service voice context during live playback");
                    update_client
                        .update_voice_context(&context)
                        .await
                        .context("forward refreshed service voice context through Twilight adapter")?;
                    if let Some(pending) = pending_voice_context_proof.take() {
                        let _ = pending.respond_to.send(Ok(context));
                    }
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
                let stats = accumulator.into_stats()
                    .context("observer audio proof completed without validated packets")?;
                *snapshot.lock().unwrap() = Some(stats.clone());
                if observer_thresholds_satisfied(&stats, expected_duration_ms) {
                    return Ok(stats);
                }

                return Err(observer_threshold_error(Some(&stats), expected_duration_ms));
            }
            command = proof_commands.recv(), if !proof_commands_closed => {
                match command {
                    Some(ObserverAudioProofCommand::ProveSpeakingStarted { respond_to }) => {
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
                    Some(ObserverAudioProofCommand::ProvePause { respond_to }) => {
                        let result = prove_observer_pause_silence(
                            observer_session,
                            expected_user_id,
                            &mut accumulator,
                            &snapshot,
                            &mut audio_started,
                        )
                        .await;
                        if let Ok(proof) = &result {
                            if proof.speaking_stopped {
                                info!(
                                    silence_ms = proof.silence_ms,
                                    "observer proved Pause by observing service speaking disappearance and no service audio",
                                );
                            }
                        }
                        let _ = respond_to.send(result);
                        last_stats = Some(accumulator.stats());
                    }
                    Some(ObserverAudioProofCommand::ProveResume { armed, respond_to }) => {
                        observer_session.set_dave_proposal_authoring(true);
                        let _ = armed.send(());
                        let result = prove_observer_resume_audio(
                            observer_session,
                            expected_user_id,
                            &mut accumulator,
                            &snapshot,
                            &mut audio_started,
                        )
                        .await;
                        observer_session.set_dave_proposal_authoring(false);
                        if let Ok(proof) = &result {
                            info!(
                                observed_packet_count = proof.observed_packet_count,
                                "observer proved Resume by observing service speaking start and receiving service audio",
                            );
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

async fn prove_observer_pause_silence(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
    accumulator: &mut AudioValidationAccumulator,
    snapshot: &Arc<Mutex<Option<AudioValidationStats>>>,
    audio_started: &mut Option<oneshot::Sender<()>>,
) -> Result<ObserverPauseProof> {
    let proof_started = Instant::now();
    let mut speaking_stopped = false;
    let mut silence_deadline = None;
    let transition_deadline = Instant::now() + OBSERVER_SPEAKING_STATE_TIMEOUT;

    loop {
        let active_deadline = silence_deadline.unwrap_or(transition_deadline);
        let Some(remaining) = active_deadline.checked_duration_since(Instant::now()) else {
            if speaking_stopped {
                return Ok(ObserverPauseProof {
                    silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                    speaking_stopped,
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
                let stats =
                    record_observer_audio_frame(frame, accumulator, snapshot, audio_started)?;
                if !speaking_stopped {
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
                if !speaking_stopped {
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
                if state.speaking & SPEAKING_FLAG_MICROPHONE != 0 {
                    if speaking_stopped {
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
                speaking_stopped = true;
                silence_deadline = Some(Instant::now() + PAUSE_OBSERVER_SILENCE_DURATION);
            }
            Ok(ObservedVoiceActivity::Disconnect(user_id)) => {
                info!(
                    user_id = %user_id,
                    "observer saw service voice client disconnect during Pause proof"
                );
                speaking_stopped = true;
                silence_deadline = Some(Instant::now() + PAUSE_OBSERVER_SILENCE_DURATION);
            }
            Err(error) if is_voice_receive_timeout(&error) => {
                if speaking_stopped {
                    return Ok(ObserverPauseProof {
                        silence_ms: duration_ms(PAUSE_OBSERVER_SILENCE_DURATION),
                        speaking_stopped,
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
) -> Result<ObserverResumeProof> {
    let start_count = accumulator.stats().observed_packet_count;
    let mut speaking_started = false;
    let deadline = Instant::now() + RESUME_OBSERVER_AUDIO_TIMEOUT;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            bail!(
                "observer did not receive resumed service speaking state and audio within {} seconds (speaking_started={} observed_after_resume={} required={})",
                RESUME_OBSERVER_AUDIO_TIMEOUT.as_secs(),
                speaking_started,
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
                let stats =
                    record_observer_audio_frame(frame, accumulator, snapshot, audio_started)?;
                let observed_after_resume = stats.observed_packet_count.saturating_sub(start_count);
                if speaking_started && observed_after_resume >= RESUME_OBSERVER_PACKET_TARGET {
                    return Ok(ObserverResumeProof {
                        observed_packet_count: observed_after_resume,
                        speaking_started: true,
                    });
                }
            }
            Ok(ObservedVoiceActivity::RtpPacket(packet)) => {
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
                    "observer timed out waiting for resumed service speaking state and audio (speaking_started={} observed_after_resume={} required={})",
                    speaking_started,
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
    let stats = accumulator
        .observe_packet(ObservedOpusPacket {
            sequence: frame.sequence,
            payload: frame.payload.as_ref(),
        })
        .context("analyze observer audio packet")?;
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
        && stats.non_silent_audio_ms >= MIN_NON_SILENT_AUDIO_MS
}

fn required_observer_decoded_audio_ms(expected_duration_ms: u64) -> u64 {
    let ratio_floor =
        expected_duration_ms.saturating_mul(OBSERVER_AUDIO_DURATION_MIN_RATIO_PERCENT) / 100;
    let tolerance_floor = expected_duration_ms.saturating_sub(OBSERVER_AUDIO_DURATION_TOLERANCE_MS);
    ratio_floor
        .min(tolerance_floor)
        .max(MIN_DECODED_AUDIO_MS.min(expected_duration_ms))
}

fn observer_threshold_error(
    stats: Option<&AudioValidationStats>,
    expected_duration_ms: u64,
) -> anyhow::Error {
    let required_decoded_audio_ms = required_observer_decoded_audio_ms(expected_duration_ms);
    match stats {
        Some(stats) => anyhow!(
            "observer audio proof finished before thresholds (observed_packet_count={} decoded_audio_ms={} required_decoded_audio_ms={} expected_duration_ms={} non_silent_audio_ms={} required_non_silent_audio_ms={})",
            stats.observed_packet_count,
            stats.decoded_audio_ms,
            required_decoded_audio_ms,
            expected_duration_ms,
            stats.non_silent_audio_ms,
            MIN_NON_SILENT_AUDIO_MS,
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

async fn wait_for_play_completed_contract_with_controls(
    events: &mut (impl Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin),
    expected_video_id: &str,
    mut state: LiveContractState,
    snapshot: Arc<Mutex<LiveContractState>>,
    controls: &mut VoiceServiceClient,
    observer_audio_started: oneshot::Receiver<()>,
    observer_proof_tx: mpsc::Sender<ObserverAudioProofCommand>,
    gateway_driver_tx: mpsc::Sender<LiveGatewayDriverCommand>,
    service_gateway_sender: MessageSender,
    service_voice: ServiceVoiceContext,
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
            wait_for_observer_audio_started(&mut observer_audio_started).await?;
            request_observer_speaking_started_proof(&observer_proof_tx).await?;
            tokio::time::sleep(PRE_PAUSE_SPEAKING_SETTLE_DURATION).await;

            let pause_proof = start_observer_pause_proof(&observer_proof_tx).await?;
            controls
                .pause_and_leave(&service_gateway_sender, &service_voice)
                .await
                .context(
                    "call Pause and leave voice through Twilight adapter during live playback",
                )?;
            let pause_proof = await_observer_pause_proof(pause_proof).await?;
            observer_playback.pause_silence_ms = pause_proof.silence_ms;
            observer_playback.pause_speaking_stopped = pause_proof.speaking_stopped;
            *observer_playback_snapshot.lock().unwrap() = Some(observer_playback.clone());
            if !pause_proof.speaking_stopped {
                warn!(
                    "observer pause proof did not receive a direct voice-gateway speaking disappearance"
                );
            }
            state.mark_pause();
            update_live_contract_snapshot(&snapshot, &state);

            let resume_proof = start_observer_resume_proof(&observer_proof_tx).await?;
            let voice_context_refresh =
                start_service_voice_context_refresh_proof(&gateway_driver_tx).await?;
            service_gateway_sender
                .command(&join_voice_channel(
                    service_voice.guild_id,
                    service_voice.channel_id,
                    false,
                    false,
                ))
                .context("send service gateway voice rejoin command before Resume")?;
            await_service_voice_context_refresh_proof(voice_context_refresh).await?;
            controls
                .resume()
                .await
                .context("call Resume after service voice rejoin during live playback")?;
            let resume_proof = await_observer_resume_proof(resume_proof).await?;
            observer_playback.resume_observed_packet_count = resume_proof.observed_packet_count;
            observer_playback.resume_speaking_started = resume_proof.speaking_started;
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
        .send(ObserverAudioProofCommand::ProveSpeakingStarted { respond_to })
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

async fn start_service_voice_context_refresh_proof(
    gateway_driver_tx: &mpsc::Sender<LiveGatewayDriverCommand>,
) -> Result<oneshot::Receiver<Result<ServiceVoiceContext>>> {
    let (respond_to, response) = oneshot::channel();
    gateway_driver_tx
        .send(LiveGatewayDriverCommand::WaitForServiceVoiceContext { respond_to })
        .await
        .context("request service voice context refresh proof")?;

    Ok(response)
}

async fn await_service_voice_context_refresh_proof(
    response: oneshot::Receiver<Result<ServiceVoiceContext>>,
) -> Result<ServiceVoiceContext> {
    timeout(
        AUTHENTIC_VOICE_EVENT_TIMEOUT + Duration::from_secs(5),
        response,
    )
    .await
    .context("timed out waiting for service voice context refresh proof")?
    .context("live gateway driver ended before service voice context refresh proof")?
}

async fn start_observer_pause_proof(
    observer_proof_tx: &mpsc::Sender<ObserverAudioProofCommand>,
) -> Result<oneshot::Receiver<Result<ObserverPauseProof>>> {
    let (respond_to, response) = oneshot::channel();
    observer_proof_tx
        .send(ObserverAudioProofCommand::ProvePause { respond_to })
        .await
        .context("request observer Pause proof")?;

    Ok(response)
}

async fn await_observer_pause_proof(
    response: oneshot::Receiver<Result<ObserverPauseProof>>,
) -> Result<ObserverPauseProof> {
    timeout(
        OBSERVER_SPEAKING_STATE_TIMEOUT + PAUSE_OBSERVER_SILENCE_DURATION + Duration::from_secs(5),
        response,
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
        .send(ObserverAudioProofCommand::ProveResume { armed, respond_to })
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
    state: &mut LiveContractState,
    snapshot: &Arc<Mutex<LiveContractState>>,
) -> Result<()> {
    client
        .stop()
        .await
        .context("call Stop through Twilight adapter after natural TrackEnded")?;
    state.mark_stop();
    update_live_contract_snapshot(snapshot, state);

    client
        .leave_voice()
        .await
        .context("call LeaveVoice through Twilight adapter after Stop")?;
    state.mark_leave_voice();
    update_live_contract_snapshot(snapshot, state);

    state.ensure_complete()?;
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
    sender
        .command(&leave_voice_channel(guild_id))
        .context("failed to send gateway leave command")?;

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
            if current_user_absent_from_guild_voice(http, guild_id).await? {
                return Ok(());
            }

            bail!("timed out waiting for leave confirmation");
        }

        if now >= next_voice_state_poll {
            if current_user_absent_from_guild_voice(http, guild_id).await? {
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
            if current_user_absent_from_guild_voice(http, guild_id).await? {
                return Ok(());
            }

            bail!("gateway shard ended before leave confirmation");
        };

        let event = match item {
            Ok(event) => event,
            Err(source) => {
                if is_fatal_gateway_receive_error(&source) {
                    if current_user_absent_from_guild_voice(http, guild_id).await? {
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
        (Err(primary), Err(cleanup)) => Err(anyhow!("{primary}; cleanup also failed: {cleanup}")),
    }
}

fn build_success_evidence(
    config: &StagingConfig,
    validated: &ValidatedLiveOutcome,
) -> LiveValidationEvidence {
    LiveValidationEvidence {
        outcome: "success".to_owned(),
        service_uri: config.discord_voice_service_uri.clone(),
        ytmusic_addr: config.discord_voice_service_ytmusic_addr.clone(),
        validated_join_voice: validated.live_contract.validated_join_voice,
        validated_update_voice_context: validated.live_contract.validated_update_voice_context,
        validated_play: validated.live_contract.validated_play,
        validated_pause: validated.live_contract.validated_pause,
        validated_resume: validated.live_contract.validated_resume,
        observer_proved_pause: validated.live_contract.observer_proved_pause,
        observer_proved_resume: validated.live_contract.observer_proved_resume,
        observer_pause_self_mute_observed: validated.observer_playback.pause_self_mute_observed,
        observer_pause_speaking_stopped: validated.observer_playback.pause_speaking_stopped,
        observer_resume_speaking_started: validated.observer_playback.resume_speaking_started,
        observer_pause_silence_ms: validated.observer_playback.pause_silence_ms,
        observer_resume_packet_count: validated.observer_playback.resume_observed_packet_count,
        validated_stop: validated.live_contract.validated_stop,
        validated_leave_voice: validated.live_contract.validated_leave_voice,
        validated_get_state: validated.live_contract.validated_get_state,
        validated_subscribe_events: validated.live_contract.validated_subscribe_events,
        saw_voice_connecting: validated.live_contract.saw_voice_connecting,
        saw_voice_ready: validated.live_contract.saw_voice_ready,
        saw_track_resolving: validated.live_contract.saw_track_resolving,
        saw_playing: validated.live_contract.saw_playing,
        saw_track_ended: validated.live_contract.saw_track_ended,
        observed_packet_count: validated.audio_stats.observed_packet_count,
        decoded_audio_ms: validated.audio_stats.decoded_audio_ms,
        non_silent_audio_ms: validated.audio_stats.non_silent_audio_ms,
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

    LiveValidationEvidence {
        outcome: "failure".to_owned(),
        service_uri: config.discord_voice_service_uri.clone(),
        ytmusic_addr: config.discord_voice_service_ytmusic_addr.clone(),
        validated_join_voice: live_contract.is_some_and(|state| state.validated_join_voice),
        validated_update_voice_context: live_contract
            .is_some_and(|state| state.validated_update_voice_context),
        validated_play: live_contract.is_some_and(|state| state.validated_play),
        validated_pause: live_contract.is_some_and(|state| state.validated_pause),
        validated_resume: live_contract.is_some_and(|state| state.validated_resume),
        observer_proved_pause: live_contract.is_some_and(|state| state.observer_proved_pause),
        observer_proved_resume: live_contract.is_some_and(|state| state.observer_proved_resume),
        observer_pause_self_mute_observed: observer_playback
            .is_some_and(|proof| proof.pause_self_mute_observed),
        observer_pause_speaking_stopped: observer_playback
            .is_some_and(|proof| proof.pause_speaking_stopped),
        observer_resume_speaking_started: observer_playback
            .is_some_and(|proof| proof.resume_speaking_started),
        observer_pause_silence_ms: observer_playback.map_or(0, |proof| proof.pause_silence_ms),
        observer_resume_packet_count: observer_playback
            .map_or(0, |proof| proof.resume_observed_packet_count),
        validated_stop: live_contract.is_some_and(|state| state.validated_stop),
        validated_leave_voice: live_contract.is_some_and(|state| state.validated_leave_voice),
        validated_get_state: live_contract.is_some_and(|state| state.validated_get_state),
        validated_subscribe_events: live_contract
            .is_some_and(|state| state.validated_subscribe_events),
        saw_voice_connecting: live_contract.is_some_and(|state| state.saw_voice_connecting),
        saw_voice_ready: live_contract.is_some_and(|state| state.saw_voice_ready),
        saw_track_resolving: live_contract.is_some_and(|state| state.saw_track_resolving),
        saw_playing: live_contract.is_some_and(|state| state.saw_playing),
        saw_track_ended: live_contract.is_some_and(|state| state.saw_track_ended),
        observed_packet_count: audio_stats.map_or(0, |stats| stats.observed_packet_count),
        decoded_audio_ms: audio_stats.map_or(0, |stats| stats.decoded_audio_ms),
        non_silent_audio_ms: audio_stats.map_or(0, |stats| stats.non_silent_audio_ms),
        failure_reason: Some(classify_failure_reason(error)),
    }
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
    } else if message.contains("observer") && message.contains("timed out") {
        "observer_timeout".to_owned()
    } else if message.contains("observer audio proof") && message.contains("thresholds") {
        "observer_audio_incomplete".to_owned()
    } else if message.contains("decode opus packet") || message.contains("analyze observer audio") {
        "observer_decode_failed".to_owned()
    } else if message.contains("speaker mapping") {
        "observer_speaker_mapping_missing".to_owned()
    } else if message.contains("trackended")
        || message.contains("playing")
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
    use discord_voice_service_twilight::SessionEventKind;
    use discord_voice_service_voice::test_support::{
        OPUS_SILENCE_FRAME, ProtectionContext, RtpPacketBuilder,
    };
    use futures::stream;
    use std::collections::HashMap;

    fn event(kind: SessionEventKind, current_video_id: Option<&str>) -> SessionEvent {
        SessionEvent {
            kind,
            current_video_id: current_video_id.map(str::to_owned),
            ..SessionEvent::default()
        }
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
        ]))
        .expect("config should parse")
    }

    #[tokio::test]
    async fn waits_for_voice_ready_before_continuing_live_contract() {
        let mut events = stream::iter(vec![
            Ok(event(SessionEventKind::VoiceConnecting, None)),
            Ok(event(SessionEventKind::VoiceReady, None)),
            Ok(event(SessionEventKind::TrackResolving, Some("video"))),
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
        assert!(final_state.saw_playing);
        assert!(final_state.saw_track_ended);
    }

    #[tokio::test]
    async fn play_completed_contract_requires_post_play_playing_after_pre_play_playing() {
        let mut events = stream::iter(vec![
            Ok(event(SessionEventKind::Playing, Some("video"))),
            Ok(event(SessionEventKind::VoiceReady, None)),
            Ok(event(SessionEventKind::TrackResolving, Some("video"))),
            Ok(event(SessionEventKind::Playing, Some("video"))),
            Ok(event(SessionEventKind::TrackEnded, Some("video"))),
        ]);

        let initial =
            wait_for_initial_voice_ready(&mut events, "video", LiveContractState::default())
                .await
                .expect("voice ready should preserve earlier playing progress");
        assert!(initial.saw_voice_ready);
        assert!(initial.saw_playing);

        let post_play_state = post_play_live_contract_state(&initial);
        assert!(post_play_state.saw_voice_ready);
        assert!(
            !post_play_state.saw_playing,
            "pre-Play Playing must not satisfy the post-Play contract",
        );
        assert!(!post_play_state.saw_track_ended);

        let final_state = wait_for_play_completed_contract(&mut events, "video", post_play_state)
            .await
            .expect("a post-Play TrackEnded event should satisfy the play-completion contract");
        assert!(final_state.saw_voice_ready);
        assert!(final_state.saw_playing);
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
    fn observer_thresholds_require_audio_near_expected_duration() {
        let expected_duration_ms = 180_000;
        let required_decoded_audio_ms = required_observer_decoded_audio_ms(expected_duration_ms);
        assert_eq!(required_decoded_audio_ms, 162_000);

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
            non_silent_audio_ms: 120_000,
            ..Default::default()
        };
        assert!(observer_thresholds_satisfied(
            &full_stats,
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
    async fn pause_proof_allows_silence_after_speaking_disappears() {
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

        assert!(proof.speaking_stopped);
        assert_eq!(proof.silence_ms, 600);
        assert_eq!(accumulator.stats().observed_packet_count, 0);
    }

    #[tokio::test]
    async fn pause_proof_allows_voice_disconnect_during_silence() {
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
        let (proof, ()) = tokio::join!(proof, send_disconnect);
        let proof = proof.unwrap();

        assert!(proof.speaking_stopped);
        assert_eq!(proof.silence_ms, 600);
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
    async fn resume_proof_request_waits_until_observer_is_armed() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut request = tokio::spawn(async move { start_observer_resume_proof(&tx).await });

        let Some(ObserverAudioProofCommand::ProveResume { armed, respond_to }) = rx.recv().await
        else {
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
                saw_playing: true,
                saw_track_ended: true,
                ..LiveContractState::default()
            }),
            audio_stats: None,
            observer_playback: Some(ObserverPlaybackProof {
                pause_silence_ms: 600,
                pause_self_mute_observed: false,
                pause_speaking_stopped: true,
                resume_speaking_started: false,
                resume_observed_packet_count: 0,
            }),
        };

        let evidence = build_failure_evidence(
            &valid_test_config(),
            &anyhow!("observer audio proof timed out"),
            Some(&snapshot),
        );

        assert!(evidence.saw_voice_ready);
        assert!(evidence.saw_playing);
        assert!(evidence.saw_track_ended);
        assert!(!evidence.observer_pause_self_mute_observed);
        assert!(evidence.observer_pause_speaking_stopped);
        assert!(!evidence.observer_resume_speaking_started);
        assert_eq!(evidence.observer_pause_silence_ms, 600);
        assert_eq!(evidence.observer_resume_packet_count, 0);
    }

    #[tokio::test]
    async fn failure_evidence_keeps_play_progress_from_snapshot_updates() {
        let initial = LiveContractState {
            saw_voice_ready: true,
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
