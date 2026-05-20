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
- A branch and PR gate in [`.github/workflows/fake-peer-ci.yml`](.github/workflows/fake-peer-ci.yml) that runs the fake-peer verification suite on pushes, pull requests, and merge-queue checks
- A protected self-hosted live validation workflow in [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) that exercises the real staging controller against Discord
- A release workflow in [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml) that only publishes the GHCR image after both fake-peer CI and live staging gates are satisfied

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

Real Discord validation is self-hosted first. [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) is the protected operator workflow for that gate. It supports manual `workflow_dispatch` runs and is also called as a reusable workflow from [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml) when a GitHub release is published.

The current workflow is intentionally honest about scope: both trigger paths validate the checked-out `discord-voice-service` source build on the self-hosted runner, not the published GHCR image for this repository. The release path checks out the published release tag, resolves the tagged commit, and rebuilds it locally before running the live Discord validation.

GitHub Actions cannot express `needs` edges across separate top-level workflows. The release-ready gate is therefore enforced in a pragmatic but technically correct way:

1. [`.github/workflows/fake-peer-ci.yml`](.github/workflows/fake-peer-ci.yml) is the normal branch, PR, and merge-queue gate and should be configured as the required status check in branch protection or repository rulesets.
2. [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) runs on a protected self-hosted runner and uses the `live-staging` environment so real secrets and required reviewers can guard the live Discord check.
3. [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml) treats release publication as not ready until it verifies a successful `Fake Peer CI` run for the tagged commit, then calls the reusable live-staging workflow, and only then builds and pushes the GHCR image.

The self-hosted runner must already have:

- support for Node 24-backed GitHub JavaScript actions before the workflow reaches any shell step; `actions/checkout@v5` depends on that runner capability
- a runner-local `browser.json` outside the workspace, exposed through a repository variable or runner environment variable named `STAGING_BROWSER_JSON_SOURCE_PATH`; the workflow copies that file into `${GITHUB_WORKSPACE}/browser.json` after checkout
- the Rust toolchain with `cargo` and `rustc`
- Podman
- outbound internet access plus inbound UDP replies suitable for real Discord voice validation

The staging environment must use dedicated Discord resources:

- a dedicated bot token
- a dedicated test guild
- a dedicated non-stage voice channel

The `live-staging` environment should hold the real Discord secrets and use required reviewers if release publication must wait for an explicit operator approval before the live gate starts.

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

For a manual or local staging run, the operator command is:

```bash
cargo run --bin staging_live_check
```

The workflow also accepts a configurable `ytmusic-service` image ref. It resolves in this order:

1. `workflow_dispatch` input `ytmusic_service_image_ref`
2. repository or environment variable `YTMUSIC_SERVICE_IMAGE_REF`
3. default `ghcr.io/ghfhffh12345/ytmusic-service:latest`

For reproducible staging, pin `YTMUSIC_SERVICE_IMAGE_REF` to an immutable tag or digest instead of relying on `:latest`.

The workflow intentionally starts the live dependencies itself instead of assuming external staging processes:

1. check out the requested commit cleanly
2. rely on the runner's pre-existing Node 24 action support for checkout, then validate the browser-config source path, Podman, and the Rust toolchain
3. copy the runner-local browser config into `${GITHUB_WORKSPACE}/browser.json`
4. build the service and staging controller binaries from the checked-out source
5. start `ytmusic-service` in Podman with `./browser.json`
6. start the built `discord-voice-service` binary from the checked-out repository
7. run the built `staging_live_check` binary
8. tear down the container, remove `${GITHUB_WORKSPACE}/browser.json`, and stop the service process

The workflow also handles one current contract wrinkle explicitly: `staging_live_check` expects `DISCORD_VOICE_SERVICE_ADDR` as a URI, while the service process still binds from the same variable name as a bare socket address. The workflow keeps the controller contract at `http://127.0.0.1:55051` and overrides the service-start step to bind on `127.0.0.1:55051`.

Every successful live staging validation should record this evidence in the implementation or release notes:

- commit SHA tested
- runner type used
- whether `ytmusic-service` was started with `./browser.json`
- whether authentic voice context was acquired
- whether `VoiceReady`, `Playing`, and `TrackEnded` were observed
- whether the 5-second live interval passed
- whether cleanup succeeded

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
