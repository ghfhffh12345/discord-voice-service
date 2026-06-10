# discord-voice-service

`discord-voice-service` is a Rust gRPC microservice intended to own Discord Voice playback for a single guild while working alongside [`ytmusic-service`](https://github.com/ghfhffh12345/ytmusic-service).

The main bot is expected to keep user-facing music features such as search, radio logic, and Discord command handling. When it is time to play a track, the bot forwards Discord voice-session context and a `videoId` to this service, which is responsible for voice-session control and the audio delivery pipeline.

## Current status

This service is production-ready for controlled single-guild, single-session use within the supported operating envelope published below.

- The gRPC control surface is implemented, including `SubscribeEvents` and `UpdateVoiceContext`.
- Startup config, health checks, container packaging, and the single-session supervisor/runtime are in place.
- Playback resolves Discord-compatible WebM/Opus sources through `ytmusic-service`, performs the runtime join path, and emits session events as state changes occur.
- Integration coverage includes a fake Discord gateway/UDP peer that verifies voice join, speaking notification, RTP/UDP audio emission, and gRPC event streaming end to end.
- Live release publication is gated by fake-peer CI plus protected staging validation of the exact container artifact that will be promoted.

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

- One gRPC listener on `DISCORD_VOICE_SERVICE_BIND_ADDR`
- Standard gRPC health checks on the same listener
- `discordvoice.v1.DiscordVoiceControl` with these RPCs: `JoinVoice`, `UpdateVoiceContext`, `Play`, `Pause`, `Resume`, `Stop`, `LeaveVoice`, `GetState`, `GetPlaybackMetrics`, `SubscribeEvents`
- Runtime event emission for voice/session state transitions such as `VoiceConnecting`, `VoiceReady`, `TrackResolving`, `Buffering`, `Playing`, `Paused`, and `TrackEnded`
- A real runtime playback path that connects to a Discord voice endpoint, performs UDP discovery, sends speaking updates, and emits Opus RTP frames for supported sources
- Distroless container packaging in [`Containerfile`](Containerfile)
- A branch and PR gate in [`.github/workflows/fake-peer-ci.yml`](.github/workflows/fake-peer-ci.yml) that runs the fake-peer verification suite on pushes, pull requests, and merge-queue checks
- A protected GitHub-hosted live validation workflow in [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) that exercises the real staging controller against Discord
- A release workflow in [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml) that only publishes the GHCR image after both fake-peer CI and live staging gates are satisfied

## Supported envelope

- one process, one active voice session
- one deployment, one guild
- Opus-in-WebM passthrough only
- GitHub-hosted live validation as a supported release constraint

This README intentionally does not broaden support claims into multi-guild scheduling, high-availability failover, or general-purpose Discord media handling.

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
| `DISCORD_VOICE_SERVICE_BIND_ADDR` | Bind address for the gRPC control listener and health checks used by the service runtime | `127.0.0.1:55051` |
| `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` | Base gRPC endpoint for `ytmusic-service` | `http://127.0.0.1:50051` |

Startup fails if either variable is missing or if `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is not a valid `http://` or `https://` URI with an authority.

The staging controller is a separate contract. It reaches this service through `DISCORD_VOICE_SERVICE_URI`, while the service process itself binds from `DISCORD_VOICE_SERVICE_BIND_ADDR`.

## Run with Podman

If you already have Podman, this is the fastest way to start the service next to `ytmusic-service`. Podman will pull `ghcr.io/ghfhffh12345/discord-voice-service:latest` automatically if it is not already present locally. The example below assumes a shared network and that `ytmusic-service` is reachable as `http://ytmusic-service:50051`.

```bash
podman run --rm \
  --name discord-voice-service \
  --network music-stack \
  -p 55051:55051 \
  -e DISCORD_VOICE_SERVICE_BIND_ADDR=0.0.0.0:55051 \
  -e DISCORD_VOICE_SERVICE_YTMUSIC_ADDR=http://ytmusic-service:50051 \
  ghcr.io/ghfhffh12345/discord-voice-service:latest
```

Use the source-based path below if you want to run the service from this repository instead of the published container image.

## Run from source

```bash
git clone https://github.com/ghfhffh12345/discord-voice-service.git
cd discord-voice-service

export DISCORD_VOICE_SERVICE_BIND_ADDR=127.0.0.1:55051
export DISCORD_VOICE_SERVICE_YTMUSIC_ADDR=http://127.0.0.1:50051

cargo run -p discord-voice-service
```

## Live staging validation

Real Discord validation runs in GitHub-hosted CI. [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) is the protected live gate, and [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml) does not continue release publication until the candidate manifest digest has passed fake-peer CI and protected live staging.

The live gate validates the exact container artifact that will be promoted. Release publication builds native `linux/amd64` and `linux/arm64` image archives, publishes those archives from the manifest job, assembles a candidate GHCR manifest, passes that candidate manifest digest into the protected live workflow, and applies the public tags only after the same digest succeeds in staging.

The release-ready contract is:

1. [`.github/workflows/fake-peer-ci.yml`](.github/workflows/fake-peer-ci.yml) must pass for the release commit.
2. [`.github/workflows/live-staging.yml`](.github/workflows/live-staging.yml) must validate the exact container artifact on the protected `live-staging` environment.
3. [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml) promotes the already-validated candidate manifest digest to the public GHCR tags.

The staging environment must provide:

- `APPLICATION_ID`
- `BOT_TOKEN`
- `OBSERVER_BOT_TOKEN`
- `TEST_GUILD_ID`
- `TEST_VOICE_CHANNEL_ID`
- `TEST_VIDEO_ID`
- `BROWSER_JSON`

There is no self-hosted runner setup and no runner-local browser path variable in the hosted design. `BROWSER_JSON` stores the actual browser configuration contents, and the workflow materializes it into a temporary `browser.json` file during the run.

The `live-staging` environment should hold the real Discord secrets and use required reviewers if release publication must wait for an explicit operator approval before the live gate starts.

The live validation controller contract is:

| Variable | Purpose | Example |
| --- | --- | --- |
| `APPLICATION_ID` | Discord application ID for the dedicated staging bot | `123456789012345678` |
| `BOT_TOKEN` | Bot token for the dedicated staging bot | `discord-bot-token` |
| `OBSERVER_BOT_TOKEN` | Bot token for the muted, non-deafened observer identity that validates receive-side audio | `discord-observer-token` |
| `TEST_GUILD_ID` | Dedicated staging guild ID | `234567890123456789` |
| `TEST_VOICE_CHANNEL_ID` | Dedicated non-stage voice channel ID inside that guild | `345678901234567890` |
| `TEST_VIDEO_ID` | YouTube video ID for the dedicated validation track used by the single live staging play/pause/resume session | `dQw4w9WgXcQ` |
| `BROWSER_JSON` | Browser configuration contents materialized into a temporary `browser.json` file for `ytmusic-service` | `{"cookies":[]}` |
| `DISCORD_VOICE_SERVICE_URI` | Host-side gRPC URI used by `staging_live_check` to reach the published service port | `http://127.0.0.1:55051` |
| `DISCORD_VOICE_SERVICE_BIND_ADDR` | In-container bind address used by the `discord-voice-service` container during live staging | `0.0.0.0:55051` |
| `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` | Base gRPC endpoint reserved for the service/controller contract with `ytmusic-service` | `http://127.0.0.1:50051` |
| `LIVE_STAGING_PROFILE` | Non-secret label for the constrained staging profile | `constrained-github-hosted` |
| `LIVE_STAGING_SERVICE_CPUS` | Docker CPU limit applied to the `discord-voice-service` container | `1.0` |
| `LIVE_STAGING_CPU_CONTENTION_WORKERS` | Number of CPU-contention workers started by the staging runner | `2` |
| `LIVE_STAGING_HTTP_READ_DELAY_MS` | Per-chunk HTTP media read delay injected inside `discord-voice-service` during staging | `5` |
| `LIVE_STAGING_HTTP_READ_JITTER_MS` | Deterministic per-chunk HTTP media read jitter injected during staging | `25` |

For local real-Discord live staging, run `scripts/ci/run_local_live_staging.sh`; the helper loads secrets from `.env`, loads `BROWSER_JSON` from `./browser.json`, starts a disposable local `ytmusic-service` container and CPU-contention container, waits for `ytmusic-service` gRPC readiness, then starts a locally built `discord-voice-service` binary inside a CPU-limited container with the HTTP read stress profile before running observer validation.

For reproducible local live staging, optionally set `YTMUSIC_SERVICE_IMAGE_REF`; otherwise the helper defaults to `ghcr.io/ghfhffh12345/ytmusic-service:latest`.

Protected live staging requires `OBSERVER_BOT_TOKEN` for the muted, non-deafened observer identity that validates receive-side audio.

Inside the live workflow, `DISCORD_VOICE_SERVICE_BIND_ADDR` remains `0.0.0.0:55051`, while `DISCORD_VOICE_SERVICE_URI` remains `http://127.0.0.1:55051`.

For manual controller-only invocation against already-running dependencies with the required environment already prepared, run:

```bash
cargo run -p discord-voice-service-live-validation --bin staging_live_check
```

For manual `workflow_dispatch` live-staging runs, provide `discord_voice_service_image_ref`. The workflow also accepts an optional `ytmusic_service_image_ref` input and an optional environment variable `YTMUSIC_SERVICE_IMAGE_REF`; otherwise it falls back to `ghcr.io/ghfhffh12345/ytmusic-service:latest`.

For reproducible staging, pin `YTMUSIC_SERVICE_IMAGE_REF` to an immutable tag or digest instead of relying on `:latest`.

Passing `staging_live_check` plus the strict success evidence artifact is the authoritative live-staging signal; manual listening is not part of the acceptance criteria.
Live-staging success waits for the natural end of the single `TEST_VIDEO_ID` session before the run is treated as release-ready.
Live-staging success requires strict service-event proof for the expected `TEST_VIDEO_ID`: `VoiceConnecting`, `VoiceReady`, `TrackResolving`, `Buffering`, initial `Playing`, `Paused`, resumed `Playing`, and natural `TrackEnded`, plus ignored invalid `Resume` and ignored redundant `Pause` checks.
Live-staging success requires observer receive-side proof: authentic voice context, pause without leaving the voice channel, no service audio or speaking state during the paused interval, explicit RTP stop-silence at the pause boundary, resume without voice-channel rejoin, at least 120 observed packets, decoded audio near the expected track duration, at least 1000 ms non-silent audio, constant 980000..=1020000 ppm aggregate and rolling tempo, no steady-playback RTP buffering, no unclassified >=100 ms RTP gaps, and no reconnect/interruption/fatal error during validation.
Live-staging success requires service-side playback stability metrics from `GetPlaybackMetrics`, including raw send-event and prepared-queue evidence, RTP interval stats, sender lateness, bounded buffer depth, refill durations, zero underruns, zero rebuffers, zero dropped/late/deficit frames, zero inserted silence, zero skipped source media, and no tempo rebases.
Live-staging success runs a constrained profile with CPU contention, a service CPU limit, and slow/jittery HTTP media reads configured by the `LIVE_STAGING_*` variables.
After natural playback metrics are captured, live-staging success also starts fresh probe playbacks and validates active `UpdateVoiceContext` reconnect rollover, `Stop`, and `LeaveVoice` while those probes are actively `Playing`.
Live-staging always uploads a structured evidence artifact summarizing the constrained profile, slow/jittery HTTP read settings, ignored invalid `Resume`, ignored redundant `Pause`, observed service events, pause silence, resume packets, active reconnect rollover, active `Stop`, active `LeaveVoice`, observed packets, decoded audio, non-silent audio, receive-side tempo/buffering/gap counters, natural playback stability metrics, reconnect probe metrics, and `failure_reason`.
The runner rejects missing, non-success, or internally inconsistent evidence; a non-empty artifact alone is not sufficient.

The workflow intentionally starts the live dependencies itself instead of assuming external staging processes:

1. check out the requested commit cleanly
2. install or verify the GitHub-hosted toolchain, then validate tools, secrets, and candidate artifact identity before the live run
3. materialize `BROWSER_JSON` into `${GITHUB_WORKSPACE}/browser.json`
4. build the staging controller binary from the checked-out source with `cargo build --locked -p discord-voice-service-live-validation --bin staging_live_check`
5. start `ytmusic-service` in Docker with the staged `browser.json`
6. start the exact candidate `discord-voice-service` container artifact from GHCR
7. run the built `staging_live_check` binary
8. remove the service containers and network, then remove `${GITHUB_WORKSPACE}/browser.json`

Every successful live staging validation should record this evidence in the implementation or release notes:

- commit SHA tested
- runner type used
- candidate manifest digest
- whether `VoiceReady`, `TrackResolving`, `Buffering`, initial `Playing`, `Paused`, resumed `Playing`, and `TrackEnded` were observed for the expected video through the natural end of the validation track
- whether active probe playbacks validated `UpdateVoiceContext` reconnect rollover, `Stop`, and `LeaveVoice`
- constrained profile name, service CPU limit, CPU-contention worker count, and HTTP read delay/jitter settings
- observed packet count, decoded audio duration, non-silent audio duration, receive-side tempo ratios/windows, RTP buffering count, and RTP gap count from the uploaded observer artifact
- playback stability metrics from `GetPlaybackMetrics`, including RTP interval stats, sender lateness, buffer depth, refill durations, underruns, rebuffers, dropped/late/deficit frames, inserted silence, skipped source media, and tempo rebases
- whether the strict success evidence validator accepted the artifact
- whether cleanup succeeded

## Rollback

A rollback means promoting a previously validated GHCR digest back to the public tags. If the GitHub-hosted live-staging contract or the staging environment changed materially since that digest was last exercised, rerun [`Live Confidence`](.github/workflows/live-confidence.yml) before treating the rollback as release-ready.

## Verify and use the service

Twilight bots should prefer the workspace client adapter in [`crates/discord-voice-service-twilight`](crates/discord-voice-service-twilight). It wraps the gRPC control API with Twilight `Id<GuildMarker>`, `Id<ChannelMarker>`, and `Id<UserMarker>` types, provides join/leave `UpdateVoiceState` helpers, and tracks `VoiceStateUpdate` plus `VoiceServerUpdate` events into the authenticated voice context expected by `JoinVoice` and `UpdateVoiceContext`. The live staging controller dogfoods this adapter, so protected validation exercises the same seam intended for Twilight-based bot integrations.

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
  -import-path crates/discord-voice-service-proto/proto \
  -proto crates/discord-voice-service-proto/proto/discordvoice/v1/control.proto \
  -d '{}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/GetState
```

Install Discord voice session details from the main bot:

```bash
grpcurl -plaintext \
  -import-path crates/discord-voice-service-proto/proto \
  -proto crates/discord-voice-service-proto/proto/discordvoice/v1/control.proto \
  -d '{
    "voice": {
      "guildId": "123456789012345678",
      "channelId": "234567890123456789",
      "userId": "345678901234567890",
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
  -import-path crates/discord-voice-service-proto/proto \
  -proto crates/discord-voice-service-proto/proto/discordvoice/v1/control.proto \
  -d '{"videoId":"dQw4w9WgXcQ"}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/Play
```

Pause, resume, stop, or leave:

```bash
grpcurl -plaintext \
  -import-path crates/discord-voice-service-proto/proto \
  -proto crates/discord-voice-service-proto/proto/discordvoice/v1/control.proto \
  -d '{}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/Pause
```

Replace `Pause` with `Resume`, `Stop`, or `LeaveVoice` as needed.

Stream session events:

```bash
grpcurl -plaintext \
  -import-path crates/discord-voice-service-proto/proto \
  -proto crates/discord-voice-service-proto/proto/discordvoice/v1/control.proto \
  -d '{}' \
  127.0.0.1:55051 \
  discordvoice.v1.DiscordVoiceControl/SubscribeEvents
```

## Troubleshooting

- Missing environment variables: startup fails if either `DISCORD_VOICE_SERVICE_BIND_ADDR` or `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is not set.
- Invalid `ytmusic-service` endpoint: startup fails unless `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR` is a valid `http://` or `https://` URI.
- Port already in use: choose a different value for `DISCORD_VOICE_SERVICE_BIND_ADDR` or free the port.
- `grpcurl list` fails: reflection is not registered, so use `-import-path` and `-proto` instead.
- Voice playback only accepts Discord-friendly WebM/Opus formats that satisfy the selector policy; unsupported formats fail instead of transcoding.
- Live staging failures should be diagnosed against the runner contract in [`docs/operations/live-staging-runner.md`](docs/operations/live-staging-runner.md) before rerunning the protected gate.

## Further reference

- [`crates/discord-voice-service-proto/proto/discordvoice/v1/control.proto`](crates/discord-voice-service-proto/proto/discordvoice/v1/control.proto)
- [`docs/operations/live-staging-runner.md`](docs/operations/live-staging-runner.md)
- [ghfhffh12345/ytmusic-service](https://github.com/ghfhffh12345/ytmusic-service)
