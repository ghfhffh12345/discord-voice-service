Use Serena for project context and Context7 for library/docs context.
Never commit `docs/superpowers/specs/*.md` or `docs/superpowers/plans/*.md`; treat them as local working documents only.

Repository overview:
- `discord-voice-service` is a Rust 2024 workspace centered on the `crates/discord-voice-service` gRPC service for single-guild Discord voice playback alongside `ytmusic-service`.
- `crates/discord-voice-service` owns runtime config, readiness/health, the `discordvoice.v1.DiscordVoiceControl` surface, single-session supervision, Discord voice transport, media playback, observability hooks, and the upstream `ytmusic-service` client.
- Repository-level files and directories:
  - `crates/discord-voice-service-live-validation/src/main.rs`: staging live-validation controller binary (`staging_live_check`).
  - `crates/discord-voice-service-proto/proto/`: protobuf contracts for `discordvoice`.
  - `crates/*/tests/`: fake-peer, runtime, transport, workflow, and contract coverage with fixture-backed helpers.
  - `.github/workflows/`: fake-peer CI, protected live staging, live confidence, and GHCR release promotion gates.
  - `Containerfile`: distroless container build for the service.
  - `README.md`: operator-facing service envelope, runtime contract, and staging/release notes.
  - `docs/operations/live-staging-runner.md`: supported self-hosted runner profile for live staging.
  - `scripts/ci/`: live staging and image-promotion helper scripts.
  - `vendor/`: vendored native crypto and DAVE dependencies used by the build.
