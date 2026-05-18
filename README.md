# discord-voice-service

`discord-voice-service` is a Rust gRPC microservice intended to own Discord Voice playback for a single guild while working alongside [`ytmusic-service`](https://github.com/ghfhffh12345/ytmusic-service).

The main bot is expected to keep user-facing music features such as search, radio logic, and Discord command handling. When it is time to play a track, the bot forwards Discord voice-session context and a `videoId` to this service, which is responsible for voice-session control and the audio delivery pipeline.

## Current status

This repository is in an early implementation stage.

- The gRPC control contract is present.
- Startup config, health checks, container packaging, and the basic single-session state machine are implemented.
- The YouTube Music format-selection policy for Discord-friendly WebM/Opus sources is implemented.
- `SubscribeEvents` is defined in the protobuf API but currently returns `UNIMPLEMENTED`.
- The full production voice transport, DAVE/E2EE flow, and real WebM/Opus streaming pipeline are not finished yet.

Treat the current codebase as a scaffold for the planned service, not as a production-ready Discord audio replacement yet.

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
- `discordvoice.v1.DiscordVoiceControl` with these RPCs: `JoinVoice`, `Play`, `Pause`, `Resume`, `Stop`, `LeaveVoice`, `GetState`
- `SubscribeEvents` (currently unimplemented)
- Distroless container packaging in [`Containerfile`](Containerfile)
- A release-triggered GHCR image workflow in [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml)

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

## Troubleshooting

- Missing environment variables: startup fails if either `DISCORD_VOICE_SERVICE_ADDR` or `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is not set.
- Invalid `ytmusic-service` endpoint: startup fails unless `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is a valid `http://` or `https://` URI.
- Port already in use: choose a different value for `DISCORD_VOICE_SERVICE_ADDR` or free the port.
- `grpcurl list` fails: reflection is not registered, so use `-import-path` and `-proto` instead.
- `SubscribeEvents` fails: this RPC is present in the contract but not implemented yet.
- Playback behavior is incomplete: the current repo still contains placeholder pieces for the real voice transport and media pipeline.

## Further reference

- [`proto/discordvoice/v1/control.proto`](proto/discordvoice/v1/control.proto)
- [`docs/superpowers/specs/2026-05-18-discord-voice-service-design.md`](docs/superpowers/specs/2026-05-18-discord-voice-service-design.md)
- [ghfhffh12345/ytmusic-service](https://github.com/ghfhffh12345/ytmusic-service)
