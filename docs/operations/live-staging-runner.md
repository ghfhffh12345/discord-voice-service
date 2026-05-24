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
  - `TEST_GUILD_ID`
  - `TEST_VOICE_CHANNEL_ID`
  - `TEST_VIDEO_ID`
  - `BROWSER_JSON`
- Optional variable:
  - `YTMUSIC_SERVICE_IMAGE_REF`

No self-hosted runner labels, runner registration, or runner-local browser file path are required.

## Preflight expectations

- The workflow verifies the secret contract, Docker/Rust tooling, and candidate artifact identity before live execution.
- The controller build command remains `cargo build --locked -p discord-voice-service-live-validation --bin staging_live_check`.
- The workflow materializes `BROWSER_JSON` into `${GITHUB_WORKSPACE}/browser.json`, mounts it into `ytmusic-service`, and removes it during cleanup.
- A preflight failure is a configuration problem, not a flaky success condition.

## Failure diagnosis

- Preflight failure: fix environment secrets, Docker/Rust tooling, or candidate artifact wiring.
- Service container failure: inspect `discord-voice-service-live-staging` logs.
- Controller failure: inspect the `staging_live_check log` workflow group or the runner temp log at `${RUNNER_TEMP}/staging-live-check.log`.
- Cleanup failure: inspect the workflow summary and rerun only after confirming the test bot has left voice.

## Rollback model

- Identify the previously validated digest.
- Promote that digest back to the public tags.
- Rerun `Live Confidence` if the GitHub-hosted live-staging contract or staging environment changed.
