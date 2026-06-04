# discord-voice-service-twilight

`discord-voice-service-twilight` is the Twilight-facing client adapter for `discord-voice-service`. It keeps the daemon crate focused on its gRPC service boundary while giving Twilight bots typed Discord IDs, gateway voice-state helpers, and a small client wrapper over the generated control API.

Use it from a Twilight bot when you want to:

- build `UpdateVoiceState` join/leave commands with `Id<GuildMarker>` and `Id<ChannelMarker>` values
- collect the authenticated voice context from `VoiceStateUpdate` plus `VoiceServerUpdate`
- call `JoinVoice`, `UpdateVoiceContext`, `Play`, `Pause`, `Resume`, `Stop`, `LeaveVoice`, `GetState`, `GetPlaybackMetrics`, and `SubscribeEvents` without hand-building protobuf messages
- receive service state/events with Twilight-typed guild and channel IDs

Sketch:

```rust,ignore
use discord_voice_service_twilight::{Client, VoiceContextTracker, join_voice_channel};
use twilight_gateway::{Event, MessageSender};
use twilight_model::id::{Id, marker::{ChannelMarker, GuildMarker, UserMarker}};

async fn join_and_play(
    sender: MessageSender,
    mut client: Client,
    guild_id: Id<GuildMarker>,
    channel_id: Id<ChannelMarker>,
    bot_user_id: Id<UserMarker>,
    mut next_event: impl AsyncFnMut() -> Option<Event>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sender.command(&join_voice_channel(guild_id, channel_id, false, false))?;

    let mut tracker = VoiceContextTracker::new(guild_id, channel_id, bot_user_id);
    while let Some(event) = next_event().await {
        if let Some(context) = tracker.observe(&event) {
            client.join_voice(context).await?;
            break;
        }
    }

    client.play("dQw4w9WgXcQ").await?;
    Ok(())
}
```

The crate also exposes a `proto` module for callers that need to drop down to the generated protobuf types.
