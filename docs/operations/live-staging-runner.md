# Live Staging Runner Contract

## Hosted runner profile

- GitHub-hosted `ubuntu-24.04` runner selected by the workflow
- Docker available on the runner for the live dependency containers
- Rust toolchain installed by the workflow before building `staging_live_check`
- `skopeo` installed by the workflow before resolving the candidate digest
- Outbound internet access and UDP voice traffic support from GitHub-hosted CI
- Protected `live-staging` GitHub environment required to supply the Discord secrets and any reviewer gate

## Required environment setup

- Secrets:
  - `APPLICATION_ID`
  - `BOT_TOKEN`
  - `OBSERVER_BOT_TOKEN`
  - `TEST_GUILD_ID`
  - `TEST_VOICE_CHANNEL_ID`
  - `TEST_VIDEO_ID` for a short dedicated validation track
  - `TEST_LONG_VIDEO_ID` for a distinct long validation track
  - `BROWSER_JSON`
- Optional variable:
  - `YTMUSIC_SERVICE_IMAGE_REF`
- Workflow-configured staging profile:
  - `LIVE_STAGING_PROFILE`
  - `LIVE_STAGING_SERVICE_CPUS`
  - `LIVE_STAGING_CPU_CONTENTION_WORKERS`
  - `LIVE_STAGING_HTTP_READ_DELAY_MS`
  - `LIVE_STAGING_HTTP_READ_JITTER_MS`
  - `LIVE_STAGING_LONG_TRACK_MIN_PACKETS`

No self-hosted runner labels, runner registration, or runner-local browser file path are required.
For local real-Discord live staging, run `scripts/ci/run_local_live_staging.sh`; the helper loads secrets from `.env`, loads `BROWSER_JSON` from `./browser.json`, starts a disposable local `ytmusic-service` container and CPU-contention container, waits for `ytmusic-service` gRPC readiness, then starts a locally built `discord-voice-service` binary inside a CPU-limited container with the HTTP read stress profile before running observer validation.
During live staging, human listeners may remain in the channel while the staging bot validates playback against the short dedicated validation track.
Protected live staging requires `OBSERVER_BOT_TOKEN` for the muted, non-deafened observer identity that validates receive-side audio.

## Preflight expectations

- The workflow verifies the secret contract, Docker/Rust tooling, and candidate artifact identity before live execution.
- The controller build command remains `cargo build --locked -p discord-voice-service-live-validation --bin staging_live_check`.
- The workflow materializes `BROWSER_JSON` into `${GITHUB_WORKSPACE}/browser.json`, mounts it into `ytmusic-service`, and removes it during cleanup.
- The preflight requires `TEST_LONG_VIDEO_ID` to be distinct from `TEST_VIDEO_ID` and requires positive constrained-profile settings, with `LIVE_STAGING_LONG_TRACK_MIN_PACKETS` at least 50.
- The runner starts CPU contention alongside the service, runs `discord-voice-service` with the configured service CPU limit, and injects the HTTP read delay/jitter profile into the service container.
- Live-staging success waits for the natural end of the validation track before the run is treated as release-ready.
- Live-staging success requires observer receive-side proof: authentic voice context, VoiceReady, Playing, pause without leaving the voice channel, no service audio or speaking state during the paused interval, resume without voice-channel rejoin, natural TrackEnded, at least 120 observed packets, at least 3000 ms decoded audio, at least 1000 ms non-silent audio, and no reconnect/interruption/fatal error during validation.
- After natural playback metrics are captured, live-staging success also starts fresh probe playbacks and validates active `UpdateVoiceContext` reconnect rollover, `Stop`, and `LeaveVoice` while those probes are actively Playing.
- Live-staging success requires service-side playback stability metrics from `GetPlaybackMetrics`, including RTP interval stats, sender lateness, buffer depth, refill durations, underruns, inserted silence, and interruption counters.
- Live-staging success runs a constrained profile with CPU contention, a service CPU limit, and slow/jittery HTTP media reads configured by the `LIVE_STAGING_*` variables.
- Live-staging success requires a distinct long-track staging probe using `TEST_LONG_VIDEO_ID`; the probe must reach at least `LIVE_STAGING_LONG_TRACK_MIN_PACKETS` RTP packets before Stop and must satisfy the same RTP interval, sender lateness, and underrun budgets.
- Live-staging always uploads a structured evidence artifact summarizing the constrained profile, slow/jittery HTTP read settings, ignored invalid Resume, ignored redundant Pause, pause silence, resume packets, active reconnect rollover, active Stop, active LeaveVoice, observed packets, decoded audio, non-silent audio, natural playback stability metrics, reconnect probe metrics, long-track metrics, and failure_reason.
- A preflight failure is a configuration problem, not a flaky success condition.

## Failure diagnosis

- Preflight failure: fix environment secrets, Docker/Rust tooling, or candidate artifact wiring.
- Service container failure: inspect `discord-voice-service-live-staging` logs.
- Controller failure: inspect the `staging_live_check log` workflow group or the runner temp log at `${RUNNER_TEMP}/staging-live-check.log` for missing voice events before the natural end of the validation track.
- Cleanup failure: inspect the workflow summary and rerun only after confirming the test bot has left voice.

## Rollback model

- Identify the previously validated digest.
- Promote that digest back to the public tags.
- Rerun `Live Confidence` if the GitHub-hosted live-staging contract or staging environment changed.
