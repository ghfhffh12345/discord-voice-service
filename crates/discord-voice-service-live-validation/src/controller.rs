use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use tokio::time::{Instant, timeout};
use tracing::{info, warn};
use twilight_gateway::error::ReceiveMessageErrorType;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_http::{Client as HttpClient, error::ErrorType as HttpErrorType};
use twilight_model::gateway::payload::outgoing::UpdateVoiceState;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, UserMarker},
};
use twilight_model::voice::VoiceState;

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_client::DiscordVoiceControlClient;
use discord_voice_service_proto::discordvoice::v1::join_voice_request::VoiceContext as ProtoVoiceContext;
use discord_voice_service_proto::discordvoice::v1::{
    JoinVoiceRequest, LeaveVoiceRequest, PlayRequest, SessionEvent, SubscribeEventsRequest,
};
use discord_voice_service_voice::{
    ObservedAudioFrame, ObservedVoiceSession, VoiceContext as ObserverVoiceContext,
};

use crate::audio::{AudioValidationStats, ObservedOpusPacket, analyze_opus_packets};
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ForwardedVoiceContext {
    guild_id: String,
    channel_id: String,
    user_id: String,
    session_id: String,
    endpoint: String,
    token: String,
}

impl ForwardedVoiceContext {
    fn to_proto_voice_context(&self) -> ProtoVoiceContext {
        ProtoVoiceContext {
            guild_id: self.guild_id.clone(),
            channel_id: self.channel_id.clone(),
            user_id: self.user_id.clone(),
            session_id: self.session_id.clone(),
            endpoint: self.endpoint.clone(),
            token: self.token.clone(),
        }
    }

    fn to_observer_voice_context(&self) -> ObserverVoiceContext {
        ObserverVoiceContext {
            guild_id: self.guild_id.clone(),
            channel_id: self.channel_id.clone(),
            user_id: self.user_id.clone(),
            session_id: self.session_id.clone(),
            endpoint: self.endpoint.clone(),
            token: self.token.clone(),
        }
    }
}

#[derive(Debug)]
struct ValidatedLiveOutcome {
    live_contract: LiveContractState,
    audio_stats: AudioValidationStats,
}

#[derive(Debug, Clone, Default)]
struct FailureEvidenceSnapshot {
    live_contract: Option<LiveContractState>,
    audio_stats: Option<AudioValidationStats>,
}

#[derive(Debug)]
struct ServiceFlowOutcome {
    result: Result<ValidatedLiveOutcome>,
    failure_snapshot: FailureEvidenceSnapshot,
    service_joined: bool,
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

    let observer_voice = match join_gateway_voice(
        &observer_sender,
        &mut observer_shard,
        GatewayJoinTarget {
            label: "observer",
            guild_id,
            channel_id,
            user_id: observer_user.id,
            self_mute: true,
            self_deaf: false,
        },
    )
    .await
    {
        Ok(context) => context,
        Err(error) => {
            let service_cleanup = cleanup_gateway_voice(
                &service_http,
                &service_sender,
                &mut service_shard,
                guild_id,
                service_user.id,
            )
            .await;
            let observer_cleanup = cleanup_gateway_voice(
                &observer_http,
                &observer_sender,
                &mut observer_shard,
                guild_id,
                observer_user.id,
            )
            .await;
            return finish_with_failure(
                &config,
                Err(error),
                combine_results(service_cleanup, observer_cleanup),
                None,
            );
        }
    };

    let mut observer_session =
        match ObservedVoiceSession::connect(observer_voice.to_observer_voice_context())
            .await
            .context("connect observer voice session")
        {
            Ok(session) => session,
            Err(error) => {
                let service_cleanup = cleanup_gateway_voice(
                    &service_http,
                    &service_sender,
                    &mut service_shard,
                    guild_id,
                    service_user.id,
                )
                .await;
                let observer_cleanup = cleanup_gateway_voice(
                    &observer_http,
                    &observer_sender,
                    &mut observer_shard,
                    guild_id,
                    observer_user.id,
                )
                .await;
                return finish_with_failure(
                    &config,
                    Err(error),
                    combine_results(service_cleanup, observer_cleanup),
                    None,
                );
            }
        };

    let flow = run_service_flow(
        &config,
        service_voice,
        &mut observer_session,
        service_user.id.to_string(),
    )
    .await;
    drop(observer_session);

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
    forwarded_voice: ForwardedVoiceContext,
    observer_session: &mut ObservedVoiceSession,
    service_user_id: String,
) -> ServiceFlowOutcome {
    let mut failure_snapshot = FailureEvidenceSnapshot::default();
    let join_client = DiscordVoiceControlClient::connect(config.discord_voice_service_uri.clone())
        .await
        .context("connect JoinVoice gRPC client");
    let mut join_client = match join_client {
        Ok(client) => client,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: false,
            };
        }
    };
    let event_client = DiscordVoiceControlClient::connect(config.discord_voice_service_uri.clone())
        .await
        .context("connect SubscribeEvents gRPC client");
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

    let response = event_client
        .subscribe_events(SubscribeEventsRequest {})
        .await
        .context("subscribe to service events");
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
                failure_snapshot,
                service_joined: false,
            };
        }
    };
    let mut events = response.into_inner();

    let join_result = join_client
        .join_voice(JoinVoiceRequest {
            voice: Some(forwarded_voice.to_proto_voice_context()),
        })
        .await
        .context("forward authentic voice context to JoinVoice");
    if let Err(error) = join_result {
        return ServiceFlowOutcome {
            result: Err(error),
            failure_snapshot,
            service_joined: false,
        };
    }

    let observer_audio_snapshot = Arc::new(Mutex::new(None::<AudioValidationStats>));
    let play_rpc = async {
        join_client
            .play(PlayRequest {
                video_id: config.test_video_id.clone(),
            })
            .await
            .context("call Play")
            .map(|_| ())
    };

    let service_contract = async {
        match timeout(
            LIVE_CONTRACT_TIMEOUT,
            assert_live_success_contract(&mut events, &config.test_video_id),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow!(
                "live contract timed out after {} seconds",
                LIVE_CONTRACT_TIMEOUT.as_secs()
            )),
        }
    };
    let observer_proof = async {
        let snapshot = Arc::clone(&observer_audio_snapshot);
        match timeout(
            LIVE_CONTRACT_TIMEOUT,
            observe_audio_proof(observer_session, &service_user_id, snapshot),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow!(
                "observer audio proof timed out after {} seconds",
                LIVE_CONTRACT_TIMEOUT.as_secs()
            )),
        }
    };
    let combined_validation = async {
        let (live_contract, audio_stats) = tokio::try_join!(service_contract, observer_proof)?;
        Ok(ValidatedLiveOutcome {
            live_contract,
            audio_stats,
        })
    };

    let result = wait_for_play_and_live_contract(combined_validation, play_rpc).await;
    failure_snapshot.audio_stats = observer_audio_snapshot.lock().unwrap().clone();

    ServiceFlowOutcome {
        result,
        failure_snapshot,
        service_joined: true,
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
    let mut contract_finished = false;
    let mut play_finished = false;
    let mut contract_state: Option<State> = None;

    tokio::pin!(contract_future);
    tokio::pin!(play_future);

    loop {
        if contract_finished && play_finished {
            return contract_state.ok_or_else(|| {
                anyhow!("internal controller error: live contract completed without state")
            });
        }

        tokio::select! {
            contract_result = &mut contract_future, if !contract_finished => {
                contract_finished = true;
                contract_state = Some(contract_result?);
            }
            play_result = &mut play_future, if !play_finished => {
                play_finished = true;
                play_result?;
            }
        }
    }
}

async fn join_gateway_voice(
    sender: &twilight_gateway::MessageSender,
    shard: &mut Shard,
    target: GatewayJoinTarget<'_>,
) -> Result<ForwardedVoiceContext> {
    sender
        .command(&UpdateVoiceState::new(
            target.guild_id,
            Some(target.channel_id),
            target.self_mute,
            target.self_deaf,
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
) -> Result<ForwardedVoiceContext> {
    let mut session_id: Option<String> = None;
    let mut token: Option<String> = None;
    let mut endpoint: Option<String> = None;

    let deadline = Instant::now() + AUTHENTIC_VOICE_EVENT_TIMEOUT;
    loop {
        if let (Some(session_id), Some(token), Some(endpoint)) =
            (session_id.as_ref(), token.as_ref(), endpoint.as_ref())
        {
            return Ok(ForwardedVoiceContext {
                guild_id: guild_id.to_string(),
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                session_id: session_id.clone(),
                endpoint: endpoint.clone(),
                token: token.clone(),
            });
        }

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

        match event {
            Event::VoiceStateUpdate(update)
                if update.user_id == user_id
                    && update.guild_id == Some(guild_id)
                    && update.channel_id == Some(channel_id) =>
            {
                session_id = Some(update.session_id.clone());
            }
            Event::VoiceServerUpdate(update) if update.guild_id == guild_id => {
                if let Some(found_endpoint) =
                    update.endpoint.as_ref().filter(|value| !value.is_empty())
                {
                    endpoint = Some(found_endpoint.clone());
                }

                if !update.token.is_empty() {
                    token = Some(update.token.clone());
                }
            }
            _ => {}
        }
    }
}

async fn observe_audio_proof(
    observer_session: &mut ObservedVoiceSession,
    expected_user_id: &str,
    snapshot: Arc<Mutex<Option<AudioValidationStats>>>,
) -> Result<AudioValidationStats> {
    let deadline = Instant::now() + LIVE_CONTRACT_TIMEOUT;
    let mut observed_frames: Vec<ObservedAudioFrame> = Vec::new();
    let mut last_stats: Option<AudioValidationStats> = None;

    loop {
        if let Some(stats) = last_stats.as_ref()
            && observer_thresholds_satisfied(stats)
        {
            return Ok(stats.clone());
        }

        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(observer_threshold_error(last_stats.as_ref()));
        };

        let frame = match observer_session
            .receive_audio_frame_from(expected_user_id, remaining)
            .await
        {
            Ok(frame) => frame,
            Err(error) => {
                let context = match last_stats.as_ref() {
                    Some(stats) => format!(
                        "observer audio proof failed after observed_packet_count={} decoded_audio_ms={} non_silent_audio_ms={}",
                        stats.observed_packet_count,
                        stats.decoded_audio_ms,
                        stats.non_silent_audio_ms
                    ),
                    None => {
                        "observer audio proof failed before any packets were validated".to_owned()
                    }
                };
                return Err(error).context(context);
            }
        };

        observed_frames.push(frame);
        let stats = analyze_opus_packets(observed_frames.iter().map(|frame| ObservedOpusPacket {
            sequence: frame.sequence,
            payload: frame.payload.as_ref(),
        }))
        .context("analyze observer audio packets")?;
        *snapshot.lock().unwrap() = Some(stats.clone());
        last_stats = Some(stats);
    }
}

fn observer_thresholds_satisfied(stats: &AudioValidationStats) -> bool {
    stats.observed_packet_count >= MIN_OBSERVED_PACKET_COUNT
        && stats.decoded_audio_ms >= MIN_DECODED_AUDIO_MS
        && stats.non_silent_audio_ms >= MIN_NON_SILENT_AUDIO_MS
}

fn observer_threshold_error(stats: Option<&AudioValidationStats>) -> anyhow::Error {
    match stats {
        Some(stats) => anyhow!(
            "observer audio proof timed out before thresholds (observed_packet_count={} decoded_audio_ms={} non_silent_audio_ms={})",
            stats.observed_packet_count,
            stats.decoded_audio_ms,
            stats.non_silent_audio_ms,
        ),
        None => anyhow!("observer audio proof timed out before any packets were received"),
    }
}

async fn assert_live_success_contract(
    events: &mut tonic::Streaming<SessionEvent>,
    expected_video_id: &str,
) -> Result<LiveContractState> {
    let mut state = LiveContractState::default();

    loop {
        let maybe_event = events.next().await;
        let event = next_session_event(maybe_event)?;
        if state.observe_event(event, expected_video_id)? {
            return Ok(state);
        }
    }
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
    let mut cleanup_client = DiscordVoiceControlClient::connect(service_addr.to_owned())
        .await
        .context("connect cleanup gRPC client")?;

    cleanup_client
        .leave_voice(LeaveVoiceRequest {})
        .await
        .context("LeaveVoice cleanup failed")?;

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
        .command(&UpdateVoiceState::new(
            guild_id,
            None::<Id<ChannelMarker>>,
            false,
            false,
        ))
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
        saw_voice_ready: validated.live_contract.saw_voice_ready,
        saw_playing: validated.live_contract.saw_playing,
        saw_track_ended: true,
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

    LiveValidationEvidence {
        outcome: "failure".to_owned(),
        service_uri: config.discord_voice_service_uri.clone(),
        ytmusic_addr: config.discord_voice_service_ytmusic_addr.clone(),
        saw_voice_ready: live_contract.is_some_and(|state| state.saw_voice_ready),
        saw_playing: live_contract.is_some_and(|state| state.saw_playing),
        saw_track_ended: false,
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
    if message.contains("observer") && message.contains("timed out") {
        "observer_timeout".to_owned()
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
