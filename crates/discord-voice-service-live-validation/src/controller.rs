use std::{future::Future, time::Duration};

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
use discord_voice_service_proto::discordvoice::v1::join_voice_request::VoiceContext;
use discord_voice_service_proto::discordvoice::v1::{
    JoinVoiceRequest, LeaveVoiceRequest, PlayRequest, SessionEvent, SubscribeEventsRequest,
};

use crate::config::StagingConfig;
use crate::contract::{
    LiveContractState, LiveValidationEvidence, emit_validation_evidence, finalize_success_evidence,
};

const AUTHENTIC_VOICE_EVENT_TIMEOUT: Duration = Duration::from_secs(45);
const GATEWAY_LEAVE_TIMEOUT: Duration = Duration::from_secs(30);
const GATEWAY_LEAVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_CONTRACT_TIMEOUT: Duration = Duration::from_secs(240);

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

pub async fn run(config: StagingConfig) -> Result<()> {
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

    finalize_success_evidence(
        flow.result,
        cleanup,
        |state| LiveValidationEvidence {
            outcome: "success".to_owned(),
            service_uri: config.discord_voice_service_uri.clone(),
            ytmusic_addr: config.discord_voice_service_ytmusic_addr.clone(),
            saw_voice_ready: state.saw_voice_ready,
            saw_playing: state.saw_playing,
            saw_track_ended: true,
            failure_reason: None,
        },
        emit_validation_evidence,
    )
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

    let result = wait_for_play_and_live_contract(live_contract, play_rpc).await;

    ServiceFlowOutcome {
        result,
        service_joined: true,
    }
}

pub async fn wait_for_play_and_live_contract<ContractFuture, PlayFuture>(
    contract_future: ContractFuture,
    play_future: PlayFuture,
) -> Result<LiveContractState>
where
    ContractFuture: Future<Output = Result<LiveContractState>>,
    PlayFuture: Future<Output = Result<()>>,
{
    let mut contract_finished = false;
    let mut play_finished = false;
    let mut contract_state: Option<LiveContractState> = None;

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
