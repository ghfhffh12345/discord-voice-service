Use Serena for project context and Context7 for library/docs context.

Project summary:
- `discord-voice-service` is a Rust gRPC microservice for single-guild Discord voice playback.
- It works alongside `ytmusic-service`, which provides YouTube Music metadata and deciphered playable URLs.
- The current repository includes real `ytmusic-service` integration, WebM/Opus fetch and demux, state/event plumbing, container packaging, fake-peer transport coverage, and the live Discord voice transport path.
- The implemented Discord path includes authentic forwarded voice context, real voice handshake, protected live transport, DAVE/E2EE runtime handling, paced 20 ms sending, and a staging live-validation controller.
- Treat the service as production-capable for controlled single-guild use, but not yet as a fully general-purpose hardened service.

Project structure overview:
- `src/api/`: gRPC service layer and request/state mapping
- `src/session/`: supervisor, runtime, readiness, events, and session state
- `src/playback/`: buffering, pacing, playback source, and recovery logic
- `src/media/`: HTTP stream, WebM demux, Opus queue, and playback position tracking
- `src/discord_voice/`: Discord voice gateway, UDP transport, RTP, speaking, crypto, DAVE, and session logic
- `src/ytmusic/`: `ytmusic-service` client and stream-format selection
- `src/bin/`: auxiliary binaries, including the staging live-validation controller
- `tests/`: protocol, runtime, recovery, transport, and fixture-backed integration tests
- `docs/superpowers/specs/`: design/spec documents
- `docs/superpowers/plans/`: implementation plans

Important repo notes:
- `docs/superpowers` is gitignored in this repository. If a task intentionally adds or updates a spec/plan there, it must be staged with `git add -f`.
- A local `.env` file is gitignored and may contain staging-only secrets for live validation.
- Live release validation is self-hosted first: fake-peer CI and protected live staging validation must pass before GHCR release publication continues.
- Current known caveat: `DISCORD_VOICE_SERVICE_ADDR` is still split between service bind-address expectations and staging controller URI expectations in the workflow/docs, so treat that contract carefully when touching staging or release automation.
