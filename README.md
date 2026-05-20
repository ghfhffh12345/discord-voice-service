# discord-voice-service

`discord-voice-service` is a Rust gRPC microservice intended to own Discord Voice playback for a single guild while working alongside [`ytmusic-service`](https://github.com/ghfhffh12345/ytmusic-service).

The main bot is expected to keep user-facing music features such as search, radio logic, and Discord command handling. When it is time to play a track, the bot forwards Discord voice-session context and a `videoId` to this service, which is responsible for voice-session control and the audio delivery pipeline.

## Current status

This repository now has a real single-session join/play path wired through the runtime.

- The gRPC control surface is implemented, including `SubscribeEvents` and `UpdateVoiceContext`.
- Startup config, health checks, container packaging, and the single-session supervisor/runtime are in place.
- Playback resolves Discord-compatible WebM/Opus sources through `ytmusic-service`, performs the runtime join path, and emits session events as state changes occur.
- Integration coverage includes a fake Discord gateway/UDP peer that verifies voice join, speaking notification, RTP/UDP audio emission, and gRPC event streaming end to end.
- Recovery and broader production hardening are still in progress, so treat this as an actively maturing service rather than a finished drop-in replacement for a general-purpose music bot.

## Intended responsibility

The target service model is:

- Single guild, single voice session per process
- Bot-assisted voice join flow
- gRPC control plane between the main bot and `discord-voice-service`
- Internal use of `ytmusic-service` for track metadata and deciphered playable URLs
- Direct Opus passthrough only
- Small bounded prebuffer ahead of Discord RTP pacing

The main bot should continue to own commands and high-level playback decisions. This service should own the voice session and media path.

## What is available today

- One gRPC listener on `DISCORD_VOICE_SERVICE_ADDR`
- Standard gRPC health checks on the same listener
- `discordvoice.v1.DiscordVoiceControl` with these RPCs: `JoinVoice`, `UpdateVoiceContext`, `Play`, `Pause`, `Resume`, `Stop`, `LeaveVoice`, `GetState`, `SubscribeEvents`
- Runtime event emission for voice/session state transitions such as `VoiceConnecting`, `VoiceReady`, `TrackResolving`, `Playing`, and `TrackEnded`
- A real runtime playback path that connects to a Discord voice endpoint, performs UDP discovery, sends speaking updates, and emits Opus RTP frames for supported sources
- Distroless container packaging in [`Containerfile`](Containerfile)
- A release-triggered GHCR image workflow in [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml)
- A self-hosted live validation workflow in [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) that exercises the real staging controller against Discord

## Playback selection policy

For v1, the playback selector is intentionally narrow.

- Only `audio/webm; codecs="opus"` formats are considered
- The format must be 48 kHz stereo
- Preferred order is `itag 250`, then `itag 249`, then another lower-bitrate Opus WebM fallback below that range
- `audio/mp4` AAC formats are excluded
- All `video/*` formats are excluded
- If no suitable Opus WebM source exists, playback should fail rather than transcode

## Before you start

You will need:

- Podman or Docker for container runs, or the Rust toolchain for local runs
- `grpcurl` if you want to inspect the health endpoint or send test RPCs
- A running `ytmusic-service` instance that this service can reach over gRPC
- A Discord bot process that can supply forwarded voice session details for `JoinVoice`

For `ytmusic-service` setup, use its own README:

- [ghfhffh12345/ytmusic-service](https://github.com/ghfhffh12345/ytmusic-service)

## Configuration

| Variable | Purpose | Example |
| --- | --- | --- |
| `DISCORD_VOICE_SERVICE_ADDR` | Bind address for the gRPC control listener and health checks | `127.0.0.1:55051` |
| `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` | Base gRPC endpoint for `ytmusic-service` | `http://127.0.0.1:50051` |

Startup fails if either variable is missing or if `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is not a valid `http://` or `https://` URI with an authority.

The current scaffold also hard-codes queue targets in code:

- Prebuffer target: 150 Opus frames
- Maximum queue depth: 300 Opus frames

## Run with Podman

If you already have Podman, this is the fastest way to start the service next to `ytmusic-service`. Podman will pull `ghcr.io/ghfhffh12345/discord-voice-service:latest` automatically if it is not already present locally. The example below assumes a shared network and that `ytmusic-service` is reachable as `http://ytmusic-service:50051`.

```bash
podman run --rm \
  --name discord-voice-service \
  --network music-stack \
  -p 55051:55051 \
  -e DISCORD_VOICE_SERVICE_ADDR=0.0.0.0:55051 \
  -e DISCORD_VOICE_SERVICE_YTMUSIC_ADDR=http://ytmusic-service:50051 \
  ghcr.io/ghfhffh12345/discord-voice-service:latest
```

Use the source-based path below if you want to run the service from this repository instead of the published container image.

## Run from source

```bash
git clone https://github.com/ghfhffh12345/discord-voice-service.git
cd discord-voice-service

export DISCORD_VOICE_SERVICE_ADDR=127.0.0.1:55051
export DISCORD_VOICE_SERVICE_YTMUSIC_ADDR=http://127.0.0.1:50051

cargo run
```

## Live staging validation

Real Discord validation is self-hosted first. [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) is the operator workflow for that gate, and it supports both manual `workflow_dispatch` runs and automatic runs after `Release Image` succeeds.

The self-hosted runner must already have:

- a runner-local `./browser.json` in the checked-out workspace; the workflow uses `actions/checkout` with `clean: false` so this gitignored file survives checkout
- the Rust toolchain with `cargo` and `rustc`
- Podman
- outbound internet access plus inbound UDP replies suitable for real Discord voice validation

The staging environment must use dedicated Discord resources:

- a dedicated bot token
- a dedicated test guild
- a dedicated non-stage voice channel

The live validation controller contract is:

| Variable | Purpose | Example |
| --- | --- | --- |
| `APPLICATION_ID` | Discord application ID for the dedicated staging bot | `123456789012345678` |
| `BOT_TOKEN` | Bot token for the dedicated staging bot | `discord-bot-token` |
| `TEST_GUILD_ID` | Dedicated staging guild ID | `234567890123456789` |
| `TEST_VOICE_CHANNEL_ID` | Dedicated non-stage voice channel ID inside that guild | `345678901234567890` |
| `TEST_VIDEO_ID` | YouTube video ID used for the live playback assertion | `dQw4w9WgXcQ` |
| `DISCORD_VOICE_SERVICE_ADDR` | gRPC URI used by `staging_live_check` to reach this service | `http://127.0.0.1:55051` |
| `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` | gRPC URI used by both the service and controller to reach `ytmusic-service` | `http://127.0.0.1:50051` |

The workflow intentionally starts the live dependencies itself instead of assuming external staging processes:

1. validate that the self-hosted runner already has `./browser.json`, Podman, and the Rust toolchain
2. start `ytmusic-service` in Podman with `./browser.json`
3. start `discord-voice-service` from the checked-out repository
4. run `cargo run --bin staging_live_check`
5. tear down the container and the local service process

The workflow also handles one current contract wrinkle explicitly: `staging_live_check` expects `DISCORD_VOICE_SERVICE_ADDR` as a URI, while the service process still binds from the same variable name as a bare socket address. The workflow keeps the controller contract at `http://127.0.0.1:55051` and overrides the service-start step to bind on `127.0.0.1:55051`.

## Verify and use the service

This service does not currently register gRPC reflection, so `grpcurl` examples should point at the bundled proto file.

Check service health:

```bash
grpcurl -plaintext \
  -d '{"service":"discordvoice.v1.DiscordVoiceControl"}' \
  127.0.0.1:55051 \
  grpc.health.v1.Health/Check
```

Fetch the current session state:

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/discordvoice/v1/control.proto \
  -d '{}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/GetState
```

Install Discord voice session details from the main bot:

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/discordvoice/v1/control.proto \
  -d '{
    "voice": {
      "guildId": "123456789012345678",
      "channelId": "234567890123456789",
      "sessionId": "voice-session-id",
      "endpoint": "us-east123.discord.media:443",
      "token": "voice-token"
    }
  }' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/JoinVoice
```

Start playback for a selected YouTube Music `videoId`:

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/discordvoice/v1/control.proto \
  -d '{"videoId":"dQw4w9WgXcQ"}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/Play
```

Pause, resume, stop, or leave:

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/discordvoice/v1/control.proto \
  -d '{}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/Pause
```

Replace `Pause` with `Resume`, `Stop`, or `LeaveVoice` as needed.

Stream session events:

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/discordvoice/v1/control.proto \
  -d '{}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/SubscribeEvents
```

## Troubleshooting

- Missing environment variables: startup fails if either `DISCORD_VOICE_SERVICE_ADDR` or `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is not set.
- Invalid `ytmusic-service` endpoint: startup fails unless `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is a valid `http://` or `https://` URI.
- Port already in use: choose a different value for `DISCORD_VOICE_SERVICE_ADDR` or free the port.
- `grpcurl list` fails: reflection is not registered, so use `-import-path` and `-proto` instead.
- Voice playback only accepts Discord-friendly WebM/Opus formats that satisfy the selector policy; unsupported formats fail instead of transcoding.
- The join/play path is real, but recovery and resilience work is still evolving, so expect behavior to keep tightening as the service matures.

## Further reference

- [`proto/discordvoice/v1/control.proto`](proto/discordvoice/v1/control.proto)
- [`docs/superpowers/specs/2026-05-18-discord-voice-service-design.md`](docs/superpowers/specs/2026-05-18-discord-voice-service-design.md)
- [ghfhffh12345/ytmusic-service](https://github.com/ghfhffh12345/ytmusic-service)
