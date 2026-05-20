use std::{collections::HashMap, env, future::Future, str::FromStr, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use discord_voice_service::proto::discordvoice::v1::discord_voice_control_client::DiscordVoiceControlClient;
use discord_voice_service::proto::discordvoice::v1::join_voice_request::VoiceContext;
use discord_voice_service::proto::discordvoice::v1::{
    JoinVoiceRequest, LeaveVoiceRequest, PlayRequest, SessionEvent, SessionEventKind,
    SubscribeEventsRequest,
};
use futures::StreamExt;
use serde::Serialize;
use tokio::time::{Instant, sleep_until, timeout};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use twilight_gateway::error::ReceiveMessageErrorType;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_http::{Client as HttpClient, error::ErrorType as HttpErrorType};
use twilight_model::gateway::payload::outgoing::UpdateVoiceState;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, UserMarker},
};
use twilight_model::voice::VoiceState;

const AUTHENTIC_VOICE_EVENT_TIMEOUT: Duration = Duration::from_secs(45);
const GATEWAY_LEAVE_TIMEOUT: Duration = Duration::from_secs(30);
const GATEWAY_LEAVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_CONTRACT_TIMEOUT: Duration = Duration::from_secs(240);
const MIN_LIVE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagingConfig {
    pub(crate) application_id: String,
    pub(crate) bot_token: String,
    pub(crate) test_guild_id: String,
    pub(crate) test_voice_channel_id: String,
    pub(crate) test_video_id: String,
    pub(crate) discord_voice_service_uri: String,
    pub(crate) discord_voice_service_ytmusic_addr: String,
}

impl StagingConfig {
    pub(crate) fn from_env() -> Result<Self> {
        Self::from_env_map(env::vars().collect())
    }

    pub(crate) fn from_env_map(env: HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            bot_token: required_env(&env, "BOT_TOKEN")?,
            application_id: required_env(&env, "APPLICATION_ID")?,
            test_guild_id: required_env(&env, "TEST_GUILD_ID")?,
            test_voice_channel_id: required_env(&env, "TEST_VOICE_CHANNEL_ID")?,
            test_video_id: required_env(&env, "TEST_VIDEO_ID")?,
            discord_voice_service_uri: required_env(&env, "DISCORD_VOICE_SERVICE_URI")?,
            discord_voice_service_ytmusic_addr: required_env(
                &env,
                "DISCORD_VOICE_SERVICE_YTMUSIC_ADDR",
            )?,
        })
    }

    pub(crate) fn guild_id(&self) -> Result<Id<GuildMarker>> {
        parse_id(&self.test_guild_id, "TEST_GUILD_ID")
    }

    pub(crate) fn channel_id(&self) -> Result<Id<ChannelMarker>> {
        parse_id(&self.test_voice_channel_id, "TEST_VOICE_CHANNEL_ID")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ForwardedVoiceContext {
    guild_id: String,
    channel_id: String,
    user_id: String,
    session_id: String,
    endpoint: String,
    token: String,
}

#[derive(Debug)]
struct ServiceFlowOutcome {
    result: Result<LiveContractState>,
    service_joined: bool,
}

#[derive(Debug, Default)]
pub(crate) struct LiveContractState {
    saw_voice_ready: bool,
    saw_playing: bool,
    satisfied_min_interval: bool,
    min_interval_deadline: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LiveValidationEvidence {
    pub(crate) outcome: String,
    pub(crate) service_uri: String,
    pub(crate) ytmusic_addr: String,
    pub(crate) saw_voice_ready: bool,
    pub(crate) saw_playing: bool,
    pub(crate) saw_track_ended: bool,
    pub(crate) satisfied_min_interval: bool,
    pub(crate) failure_reason: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    run(StagingConfig::from_env()?).await
}

pub(crate) async fn run(config: StagingConfig) -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    info!(
        application_id = %config.application_id,
        guild_id = %config.test_guild_id,
        channel_id = %config.test_voice_channel_id,
        service_uri = %config.discord_voice_service_uri,
        ytmusic_addr = %config.discord_voice_service_ytmusic_addr,
        "starting staging live controller",
    );

    let http = HttpClient::new(config.bot_token.clone());
    let current_user = http
        .current_user()
        .await
        .context("fetch current Discord user")?
        .model()
        .await
        .context("decode current Discord user response")?;
    let user_id = current_user.id;

    let mut shard = Shard::new(
        ShardId::ONE,
        config.bot_token.clone(),
        Intents::GUILD_VOICE_STATES,
    );
    let sender = shard.sender();
    let guild_id = config.guild_id()?;
    let channel_id = config.channel_id()?;
    sender
        .command(&UpdateVoiceState::new(
            guild_id,
            Some(channel_id),
            false,
            false,
        ))
        .context("send gateway voice join command")?;

    let forwarded_voice = match wait_for_authentic_voice_context(&mut shard, &config, user_id).await
    {
        Ok(context) => context,
        Err(error) => {
            let cleanup =
                cleanup_gateway_voice(&http, &sender, &mut shard, guild_id, user_id).await;
            return combine_results(Err(error), cleanup);
        }
    };

    let flow = run_service_flow(&config, forwarded_voice).await;
    let cleanup = cleanup_after_flow(
        &config.discord_voice_service_uri,
        flow.service_joined,
        &http,
        &sender,
        &mut shard,
        guild_id,
        user_id,
    )
    .await;

    let flow_result = flow.result.and_then(|state| {
        emit_validation_evidence(&LiveValidationEvidence {
            outcome: "success".to_owned(),
            service_uri: config.discord_voice_service_uri.clone(),
            ytmusic_addr: config.discord_voice_service_ytmusic_addr.clone(),
            saw_voice_ready: state.saw_voice_ready,
            saw_playing: state.saw_playing,
            saw_track_ended: true,
            satisfied_min_interval: state.satisfied_min_interval,
            failure_reason: None,
        })?;

        Ok(state)
    });

    match (flow_result, cleanup) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
        (Err(primary), Err(cleanup_error)) => {
            Err(primary.context(format!("cleanup also failed: {cleanup_error}")))
        }
    }
}

async fn run_service_flow(
    config: &StagingConfig,
    forwarded_voice: ForwardedVoiceContext,
) -> ServiceFlowOutcome {
    let join_client = DiscordVoiceControlClient::connect(config.discord_voice_service_uri.clone())
        .await
        .context("connect JoinVoice gRPC client");
    let mut join_client = match join_client {
        Ok(client) => client,
        Err(error) => {
            return ServiceFlowOutcome {
                result: Err(error),
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
                service_joined: false,
            };
        }
    };
    let mut events = response.into_inner();

    let join_result = join_client
        .join_voice(JoinVoiceRequest {
            voice: Some(VoiceContext {
                guild_id: forwarded_voice.guild_id,
                channel_id: forwarded_voice.channel_id,
                user_id: forwarded_voice.user_id,
                session_id: forwarded_voice.session_id,
                endpoint: forwarded_voice.endpoint,
                token: forwarded_voice.token,
            }),
        })
        .await
        .context("forward authentic voice context to JoinVoice");
    if let Err(error) = join_result {
        return ServiceFlowOutcome {
            result: Err(error),
            service_joined: false,
        };
    }

    let play_rpc = async {
        join_client
            .play(PlayRequest {
                video_id: config.test_video_id.clone(),
            })
            .await
            .context("call Play")
            .map(|_| ())
    };
    let live_contract = async {
        match timeout(
            LIVE_CONTRACT_TIMEOUT,
            assert_live_success_contract(&mut events),
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

    let result = wait_for_play_and_live_contract(live_contract, play_rpc).await;

    ServiceFlowOutcome {
        result,
        service_joined: true,
    }
}

pub(crate) async fn wait_for_play_and_live_contract<ContractFuture, PlayFuture>(
    contract_future: ContractFuture,
    play_future: PlayFuture,
) -> Result<LiveContractState>
where
    ContractFuture: Future<Output = Result<LiveContractState>>,
    PlayFuture: Future<Output = Result<()>>,
{
    let mut contract_finished = false;
    let mut play_finished = false;
    let mut contract_state = None;

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

async fn wait_for_authentic_voice_context(
    shard: &mut Shard,
    config: &StagingConfig,
    user_id: Id<UserMarker>,
) -> Result<ForwardedVoiceContext> {
    let guild_id = config.guild_id()?;
    let channel_id = config.channel_id()?;
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
            .ok_or_else(|| anyhow!("timed out waiting for authentic voice gateway events"))?;

        let next = timeout(remaining, shard.next_event(EventTypeFlags::all()))
            .await
            .map_err(|_| anyhow!("timed out waiting for authentic voice gateway events"))?;

        let Some(item) = next else {
            bail!("gateway shard ended before authentic voice events were observed");
        };

        let event = match item {
            Ok(event) => event,
            Err(source) => {
                if is_fatal_gateway_receive_error(&source) {
                    return Err(anyhow!(
                        "fatal gateway receive error while waiting for voice events: {source}"
                    ));
                }

                warn!(error = %source, "transient gateway receive error");
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

async fn assert_live_success_contract(
    events: &mut tonic::Streaming<SessionEvent>,
) -> Result<LiveContractState> {
    let mut state = LiveContractState::default();

    loop {
        if state.waiting_for_live_interval() {
            match state.min_interval_deadline {
                Some(deadline) if Instant::now() >= deadline => {
                    state.update_min_interval(Instant::now());
                    continue;
                }
                Some(deadline) => {
                    tokio::select! {
                        maybe_event = events.next() => {
                            let event = next_session_event(maybe_event)?;
                            if state.observe_event(event, Instant::now())? {
                                return Ok(state);
                            }
                        }
                        _ = sleep_until(deadline) => {
                            state.update_min_interval(deadline);
                        }
                    }
                }
                None => bail!("internal controller error: missing live interval deadline"),
            }
        } else {
            let maybe_event = events.next().await;
            let event = next_session_event(maybe_event)?;
            if state.observe_event(event, Instant::now())? {
                return Ok(state);
            }
        }
    }
}

pub(crate) fn emit_validation_evidence(evidence: &LiveValidationEvidence) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string(evidence).context("serialize live validation evidence")?
    );
    Ok(())
}

impl LiveContractState {
    pub(crate) fn waiting_for_live_interval(&self) -> bool {
        self.saw_playing && !self.satisfied_min_interval
    }

    pub(crate) fn update_min_interval(&mut self, now: Instant) {
        if let Some(deadline) = self.min_interval_deadline
            && now >= deadline
        {
            self.satisfied_min_interval = true;
        }
    }

    pub(crate) fn observe_event(&mut self, event: SessionEvent, now: Instant) -> Result<bool> {
        let kind = SessionEventKind::try_from(event.kind).unwrap_or(SessionEventKind::Unspecified);

        match kind {
            SessionEventKind::VoiceReady => {
                self.saw_voice_ready = true;
            }
            SessionEventKind::Playing => {
                if !self.saw_playing {
                    self.saw_playing = true;
                    self.min_interval_deadline = Some(now + MIN_LIVE_INTERVAL);
                }
            }
            SessionEventKind::TrackEnded => {
                if !self.saw_voice_ready {
                    bail!("TrackEnded observed before VoiceReady");
                }
                if !self.saw_playing {
                    bail!("TrackEnded observed before Playing");
                }
                if !self.satisfied_min_interval {
                    bail!("TrackEnded observed before 5 seconds of continuous live playback");
                }

                return Ok(true);
            }
            SessionEventKind::FatalError => {
                bail!("FatalError observed: {}", display_event_message(&event));
            }
            SessionEventKind::PlaybackInterrupted => {
                bail!(
                    "PlaybackInterrupted observed: {}",
                    display_event_message(&event)
                );
            }
            SessionEventKind::VoiceReconnecting => {
                bail!(
                    "VoiceReconnecting observed: {}",
                    display_event_message(&event)
                );
            }
            SessionEventKind::VoiceConnecting
            | SessionEventKind::TrackResolving
            | SessionEventKind::Buffering
            | SessionEventKind::Paused
            | SessionEventKind::Stopped
                if self.saw_playing =>
            {
                bail!(
                    "playback left steady Playing state after start: {}",
                    kind.as_str_name()
                );
            }
            _ => {}
        }

        Ok(false)
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

fn display_event_message(event: &SessionEvent) -> String {
    if event.message.trim().is_empty() {
        "no message".to_owned()
    } else {
        event.message.clone()
    }
}

async fn cleanup_after_flow(
    service_addr: &str,
    service_joined: bool,
    http: &HttpClient,
    sender: &twilight_gateway::MessageSender,
    shard: &mut Shard,
    guild_id: Id<GuildMarker>,
    user_id: Id<UserMarker>,
) -> Result<()> {
    let service_cleanup = if service_joined {
        cleanup_service_voice(service_addr).await
    } else {
        Ok(())
    };
    let gateway_cleanup = cleanup_gateway_voice(http, sender, shard, guild_id, user_id).await;

    combine_results(service_cleanup, gateway_cleanup)
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

async fn cleanup_gateway_voice(
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

pub(crate) async fn current_user_absent_from_guild_voice(
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

pub(crate) fn leave_confirmed_by_rest_voice_state(
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

pub(crate) fn combine_results(primary: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => Err(anyhow!("{primary}; cleanup also failed: {cleanup}")),
    }
}

fn required_env(env: &HashMap<String, String>, key: &'static str) -> Result<String> {
    match env.get(key).map(String::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        Some(_) => bail!("required env var {key} must not be empty"),
        None => bail!("missing required env var: {key}"),
    }
}

fn parse_id<T>(value: &str, field: &'static str) -> Result<Id<T>> {
    Id::<T>::from_str(value).with_context(|| format!("invalid Discord snowflake in {field}"))
}

fn is_fatal_gateway_receive_error(error: &twilight_gateway::error::ReceiveMessageError) -> bool {
    matches!(error.kind(), ReceiveMessageErrorType::Reconnect)
}
