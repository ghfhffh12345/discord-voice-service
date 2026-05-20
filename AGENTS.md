Use Serena for project context and Context7 for library/docs context.
Never commit `docs/superpowers/specs/*.md` or `docs/superpowers/plans/*.md`; treat them as local working documents only.

Project summary:
- `discord-voice-service` is a Rust gRPC microservice for single-guild Discord voice playback alongside `ytmusic-service`.
- The current repository includes real `ytmusic-service` integration, WebM/Opus fetch and demux, session/event plumbing, container packaging, fake-peer transport coverage, and the live Discord voice transport path.
- The implemented Discord path includes authentic forwarded voice context, full voice handshake, protected RTP transport, DAVE/E2EE runtime handling, paced 20 ms sending, and the staging live-validation controller.
- Treat the service as production-ready for controlled single-guild, single-session use within the published envelope, not as a hardened general-purpose media service.

Project structure overview:
- `src/api/`: gRPC service layer and request/state mapping
- `src/session/`: supervisor, runtime, readiness, events, and session state
- `src/playback/` and `src/media/`: buffering, pacing, recovery, HTTP/WebM/Opus ingestion, and playback position tracking
- `src/discord_voice/`: Discord voice gateway, UDP transport, RTP, speaking, crypto, DAVE, and session logic
- `src/ytmusic/`: `ytmusic-service` client and stream-format selection
- `src/bin/`: auxiliary binaries, including `staging_live_check`
- `proto/`: gRPC contracts for `discordvoice` and `ytmusic`
- `tests/`: protocol, runtime, recovery, transport, and fixture-backed integration tests
- `.github/workflows/`: fake-peer CI, protected live staging, live confidence, and GHCR release promotion
- `docs/operations/`: staging runner and operational notes
- `docs/superpowers/specs/` and `docs/superpowers/plans/`: local design and plan working documents
- `vendor/`: vendored native crypto and DAVE dependencies used by the build

Important repo notes:
- `docs/superpowers` is gitignored in this repository. If a task intentionally adds or updates a spec/plan there, it must be staged with `git add -f`.
- A local `.env` file is gitignored and may contain staging-only secrets for live validation.
- Live release validation is self-hosted first: fake-peer CI and protected live staging validation must pass before GHCR release publication continues.
- `DISCORD_VOICE_SERVICE_BIND_ADDR` is the service bind address; `DISCORD_VOICE_SERVICE_URI` is the staging controller target. Treat that split carefully when touching staging or release automation.
