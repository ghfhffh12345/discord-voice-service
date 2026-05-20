# Live Staging Runner Contract

## Supported runner profile

- Self-hosted Linux runner labeled for Discord voice staging
- Node 24-compatible GitHub Actions runtime
- Podman, `cargo`, `rustc`, and `skopeo` installed
- Outbound internet access and inbound UDP replies for Discord voice
- Runner-local `browser.json` available through `STAGING_BROWSER_JSON_SOURCE_PATH`

## Preflight expectations

- The workflow verifies tools, secrets, browser config, and candidate artifact identity before live execution.
- A preflight failure is a configuration problem, not a flaky success condition.

## Failure diagnosis

- Preflight failure: fix runner prerequisites or secret/config wiring.
- Service container failure: inspect `discord-voice-service-live-staging` logs.
- Controller failure: inspect the JSON evidence line from `staging_live_check`.
- Cleanup failure: inspect the workflow summary and rerun only after confirming the test bot has left voice.

## Rollback model

- Identify the previously validated digest.
- Promote that digest back to the public tags.
- Rerun `Live Confidence` if the runner or staging environment changed.
