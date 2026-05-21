# Live Staging Runner Contract

## Supported runner profile

- Self-hosted Linux runner carrying the required `self-hosted`, `linux`, and `discord-voice-staging` labels
- Node 24-compatible GitHub Actions runtime
- Podman, `cargo`, `rustc`, and `skopeo` installed
- Outbound internet access and inbound UDP replies for Discord voice
- Runner-local `browser.json` available through `STAGING_BROWSER_JSON_SOURCE_PATH`
- Protected `live-staging` GitHub environment required to supply the Discord secrets and enforce any required reviewer approvals before the live gate starts

The workflow checks out the workspace, builds `staging_live_check` from the `discord-voice-service-live-validation` package, then runs the resolved `discord-voice-service` container artifact under Podman.

## Preflight expectations

- The workflow verifies tools, secrets, browser config, and candidate artifact identity before live execution.
- The controller build command is `cargo build --locked -p discord-voice-service-live-validation --bin staging_live_check`.
- A preflight failure is a configuration problem, not a flaky success condition.

## Failure diagnosis

- Preflight failure: fix runner prerequisites or secret/config wiring.
- Service container failure: inspect `discord-voice-service-live-staging` logs.
- Controller failure: inspect the `staging_live_check log` workflow group or the runner temp log at `${RUNNER_TEMP}/staging-live-check.log`.
- Cleanup failure: inspect the workflow summary and rerun only after confirming the test bot has left voice.

## Rollback model

- Identify the previously validated digest.
- Promote that digest back to the public tags.
- Rerun `Live Confidence` if the runner or staging environment changed.
