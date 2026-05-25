use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};
use tracing::warn;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_http::Client as HttpClient;
use twilight_model::gateway::payload::outgoing::UpdateVoiceState;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, UserMarker},
};

use discord_voice_service_voice::{ObservedAudioFrame, ObservedVoiceSession, VoiceContext};

use crate::audio_match::{
    ObserverAudioEvidence, build_expected_track_frames, compare_expected_and_observed,
};
use crate::config::StagingConfig;
use crate::controller::is_fatal_gateway_receive_error;

const OBSERVER_VOICE_EVENT_TIMEOUT: Duration = Duration::from_secs(45);
const OBSERVER_AUDIO_PROOF_TIMEOUT: Duration = Duration::from_secs(240);
const OBSERVER_FRAME_RECEIVE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObserverGatewayVoiceContext {
    guild_id: String,
    channel_id: String,
    user_id: String,
    session_id: String,
    endpoint: String,
    token: String,
}

pub async fn verify_observer_audio(config: StagingConfig) -> Result<ObserverAudioEvidence> {
    verify_observer_audio_with_ready(config, None).await
}

pub(crate) async fn verify_observer_audio_with_ready(
    config: StagingConfig,
    ready: Option<oneshot::Sender<()>>,
) -> Result<ObserverAudioEvidence> {
    let http = HttpClient::new(config.observer_bot_token.clone());
    let current_user = http
        .current_user()
        .await
        .context("fetch observer Discord user")?
        .model()
        .await
        .context("decode observer Discord user response")?;
    let observer_user_id = current_user.id;

    let mut shard = Shard::new(
        ShardId::ONE,
        config.observer_bot_token.clone(),
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
        .context("send observer gateway voice join command")?;

    let context = wait_for_observer_voice_context(&mut shard, &config, observer_user_id).await?;
    let expected_frames = build_expected_track_frames(
        &config.discord_voice_service_ytmusic_addr,
        &config.test_video_id,
    )
    .await?;
    let mut session = ObservedVoiceSession::connect(context.into_voice_context())
        .await
        .context("connect observer voice session")?;
    if let Some(ready) = ready {
        let _ = ready.send(());
    }
    let result = receive_and_compare(&mut session, &expected_frames).await;

    if let Err(error) = sender
        .command(&UpdateVoiceState::new(
            guild_id,
            None::<Id<ChannelMarker>>,
            false,
            false,
        ))
        .context("send observer gateway voice leave command")
    {
        return match result {
            Ok(_evidence) => Err(error),
            Err(primary) => Err(primary.context(format!("observer cleanup also failed: {error}"))),
        };
    }

    result
}

async fn wait_for_observer_voice_context(
    shard: &mut Shard,
    config: &StagingConfig,
    user_id: Id<UserMarker>,
) -> Result<ObserverGatewayVoiceContext> {
    let guild_id = config.guild_id()?;
    let channel_id = config.channel_id()?;
    let mut session_id: Option<String> = None;
    let mut token: Option<String> = None;
    let mut endpoint: Option<String> = None;

    let deadline = Instant::now() + OBSERVER_VOICE_EVENT_TIMEOUT;
    loop {
        if let (Some(session_id), Some(token), Some(endpoint)) =
            (session_id.as_ref(), token.as_ref(), endpoint.as_ref())
        {
            return Ok(ObserverGatewayVoiceContext {
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
                anyhow!("timed out waiting for observer authentic voice gateway events")
            })?;
        let next = timeout(remaining, shard.next_event(EventTypeFlags::all()))
            .await
            .map_err(|_| {
                anyhow!("timed out waiting for observer authentic voice gateway events")
            })?;
        let Some(item) = next else {
            bail!("observer gateway shard ended before voice events were observed");
        };

        let event = match item {
            Ok(event) => event,
            Err(source) => {
                if is_fatal_gateway_receive_error(&source) {
                    return Err(anyhow!(
                        "fatal observer gateway receive error while waiting for voice events: {source}"
                    ));
                }

                warn!(error = %source, "transient observer gateway receive error");
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

async fn receive_and_compare(
    session: &mut ObservedVoiceSession,
    expected_frames: &[discord_voice_service_playback::media::opus_queue::OpusFrame],
) -> Result<ObserverAudioEvidence> {
    let deadline = Instant::now() + OBSERVER_AUDIO_PROOF_TIMEOUT;
    let mut observed = Vec::<ObservedAudioFrame>::new();
    let mut latest_evidence = compare_expected_and_observed(expected_frames, &observed);

    while Instant::now() < deadline {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        let frame_timeout = remaining.min(OBSERVER_FRAME_RECEIVE_TIMEOUT);

        match session.receive_audio_frame(frame_timeout).await {
            Ok(frame) => observed.push(frame),
            Err(error) if error.to_string().contains("timed out") => continue,
            Err(error) => return Err(error).context("receive observer audio frame"),
        }

        latest_evidence = compare_expected_and_observed(expected_frames, &observed);
        if latest_evidence.verified {
            return Ok(latest_evidence);
        }
    }

    Ok(latest_evidence)
}

impl ObserverGatewayVoiceContext {
    fn into_voice_context(self) -> VoiceContext {
        VoiceContext {
            guild_id: self.guild_id,
            channel_id: self.channel_id,
            user_id: self.user_id,
            session_id: self.session_id,
            endpoint: self.endpoint,
            token: self.token,
        }
    }
}
