Use Serena for project context and Context7 for library/docs context.
Never commit `docs/superpowers/specs/*.md` or `docs/superpowers/plans/*.md`; treat them as local working documents only.

Repository overview:
- `discord-voice-service` is a Rust 2024 Cargo workspace for single-guild Discord voice playback alongside `ytmusic-service`.
- Workspace members:
  - `crates/discord-voice-service`: service binary crate.
  - `crates/discord-voice-service-proto`: public `discordvoice.v1` protobuf/gRPC contract crate.
  - `crates/discord-voice-service-runtime`: control-plane runtime and session orchestration.
  - `crates/discord-voice-service-voice`: Discord voice transport and native crypto/DAVE integration.
  - `crates/discord-voice-service-playback`: playback pipeline and `ytmusic-service-proto` client integration.
  - `crates/discord-voice-service-live-validation`: staging live-validation controller with `staging_live_check`.
  - `crates/discord-voice-service-test-support`: shared fake peers, fixtures, and test helpers.
- Repository-level files and directories:
  - `Cargo.toml`: virtual workspace manifest with shared package metadata and dependency versions.
  - `crates/`: all first-party crates.
  - `crates/discord-voice-service-proto/proto/`: public protobuf contract for `discordvoice`.
  - `.github/workflows/`: fake-peer CI, protected live staging, live confidence, and GHCR release promotion gates.
  - `Containerfile`: distroless container build for the `discord-voice-service` app crate.
  - `README.md`: operator-facing service envelope, runtime contract, and staging/release notes.
  - `docs/operations/live-staging-runner.md`: supported self-hosted runner profile for live staging.
  - `scripts/ci/`: live staging and image-promotion helper scripts.
  - `vendor/`: vendored native crypto and DAVE dependencies used by `discord-voice-service-voice`.
