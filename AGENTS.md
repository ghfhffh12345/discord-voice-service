# Repository Guidelines

## Project Structure & Module Organization

This is a Rust 2024 Cargo workspace for a single-guild Discord voice playback service that integrates with `ytmusic-service` at `https://github.com/ghfhffh12345/ytmusic-service`. Workspace crates live under `crates/`: `discord-voice-service` is the runnable service; `runtime`, `voice`, and `playback` own session orchestration, Discord voice/RTP, and media pacing; `proto`, `twilight`, `live-validation`, and `test-support` cover gRPC, bot adapters, staging, and fakes. Integration tests are in each crate's `tests/` directory. Audio fixtures live in `crates/discord-voice-service-test-support/fixtures/`. Operational docs are in `docs/operations/`.

## Build, Test, and Development Commands

- `cargo build --workspace`: build every workspace crate.
- `cargo run -p discord-voice-service`: run the service locally after setting `DISCORD_VOICE_SERVICE_BIND_ADDR` and `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR`.
- `cargo fmt --all --check`: verify Rust formatting.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: run the same strict lint gate used by CI.
- `cargo test --workspace -v`: run the full fake-peer and integration test suite.
- `scripts/ci/run_local_live_staging.sh`: run real Discord staging locally; requires `.env`, `browser.json`, container tooling, and staging credentials.

## Coding Style & Naming Conventions

Use standard Rust formatting via `rustfmt`; do not hand-align code. Prefer explicit, domain-oriented names such as `VoiceContext`, `PlaybackMetrics`, or `UpdateVoiceContext` over abbreviations. Keep modules scoped by responsibility and follow existing crate boundaries before adding new shared abstractions. Proto files live under each crate's `proto/` directory and generated Rust should not be edited by hand.

## Testing Guidelines

Add integration tests beside the crate behavior they cover, using descriptive snake_case filenames such as `voice_handshake.rs` or `runtime_end_to_end_playback.rs`. Prefer deterministic fake Discord and fake `ytmusic-service` support from `discord-voice-service-test-support`. Live Discord validation belongs in `discord-voice-service-live-validation` or the staging scripts, not in normal unit tests.

## Commit & Pull Request Guidelines

Use Conventional Commits on one line, for example `fix(playback): preserve live media tempo` or `docs: update contributor guide`. PRs should explain the behavior change, list validation run locally, and link any related issue. Include staging evidence for changes that affect live playback, voice connection lifecycle, release workflows, or container behavior.

## Agent-Specific Instructions

Use Context7 for library or documentation context. Use `$conventional-commit` when creating commit messages.

## Security & Configuration Tips

Treat `.env` and `browser.json` as secrets. They are ignored by git and must not appear with real values in docs, logs, tests, fixtures, or commits.
