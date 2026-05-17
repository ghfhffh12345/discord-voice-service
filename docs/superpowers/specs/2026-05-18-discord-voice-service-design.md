# Discord Voice Service Design

Date: 2026-05-18
Project: `discord-voice-service`
Status: Approved design draft

## Goal

Build a lightweight Rust microservice that runs alongside the existing `ytmusic-service` in Podman and owns the full Discord Voice playback path for a single guild. The main bot remains responsible for user-facing commands, queue/radio logic, and track choice. When it is time to play a track, the bot sends a `videoId` plus forwarded Discord voice session details to `discord-voice-service`, and this service handles the rest.

The service must:

- manage the Discord voice session transport after the bot forwards voice connection details
- call `ytmusic-service` internally for playback metadata and playable URL resolution
- select a Discord Voice-compatible Opus stream for passthrough
- stream audio to Discord in real time with stable pacing
- remain small and memory-efficient enough for Podman deployment beside `ytmusic-service`

The service must not depend on Songbird, Lavalink, lavaplayer, `ffmpeg`, `yt-dlp`, or any other external media helper binary.

## Scope

### In scope for v1

- single active guild voice session per process
- full Discord voice websocket and UDP transport owned by this service
- bot-assisted voice join flow
- unary gRPC control API plus one server-streamed event subscription API
- playback commands: `join`, `play(videoId)`, `pause`, `resume`, `stop`, `leave`
- pure Opus passthrough from YouTube Music WebM audio when a supported format exists
- small bounded prebuffer ahead of RTP send
- DAVE-capable Discord voice transport, required as of March 1, 2026

### Out of scope for v1

- multi-guild concurrency
- queue ownership
- seek
- volume control
- transcode fallback
- AAC or MP4 audio playback
- video playback
- automatic playback of unsupported tracks through a decoder/encoder path

## External Dependencies

### `ytmusic-service`

The service will use the existing `ytmusic-service` public gRPC API:

- `GetSong(video_id)` to retrieve streaming metadata and adaptive formats
- `Decipher(signature_cipher)` to resolve the playable URL

Design assumption: resolving the final playable URL normally requires calling `Decipher` on the selected format's `signature_cipher`. This is part of the standard play flow, not an edge case.

### Discord

The main bot remains responsible for the primary Discord Gateway session and user-facing bot behavior. `discord-voice-service` does not discover voice session details on its own. Instead, the bot forwards the voice connection context required to complete the Discord voice handshake:

- guild ID
- channel ID
- voice `session_id`
- voice server token
- voice endpoint

Current-date protocol note: Discord's current voice documentation states that end-to-end encryption support via DAVE became required for voice conversations on March 1, 2026. Since this design is dated May 18, 2026, the voice transport must be designed with DAVE support in scope for v1 rather than as a future enhancement.

## High-Level Architecture

The service follows the Direct Passthrough Worker model with a bounded prebuffer:

1. The main bot sends control RPCs to `discord-voice-service`.
2. A single session supervisor serializes commands and owns the authoritative state machine.
3. A Discord voice transport component manages the voice websocket, UDP socket, RTP packet state, transport encryption, and DAVE-related voice protocol flow.
4. A playback worker resolves the selected YouTube Music stream through `ytmusic-service`, opens the playable URL, incrementally demuxes WebM/Opus, and fills a bounded Opus frame queue.
5. A pacer sends one Opus frame every 20 ms over Discord RTP from the queue.

This keeps the architecture simple while preventing Discord playback timing from depending directly on HTTP read timing from YouTube Music.

## Internal Components

### 1. Control API layer

Exposes the gRPC interface used by the main bot. This layer validates requests, forwards commands into the supervisor, and provides current state snapshots and event streaming to callers.

### 2. Session supervisor

Owns the single active session and serializes all state mutations. This component is responsible for:

- enforcing one-session-only semantics
- rejecting invalid commands for the current state
- replacing or stopping the active playback worker when necessary
- publishing state transitions and error events to subscribers

### 3. Discord voice transport

Owns Discord voice connectivity after `JoinVoice` installs fresh connection details from the bot. Responsibilities:

- open and maintain the Discord voice websocket
- perform UDP discovery and `Select Protocol`
- negotiate RTP-size transport encryption
- support current required voice protocol behavior, including DAVE support
- maintain SSRC, sequence, timestamp, nonce, and heartbeat state
- encrypt and send RTP voice packets
- send the required five Opus silence frames before ending transmission

Transport mode rule:

- must support `aead_xchacha20_poly1305_rtpsize`
- should prefer `aead_aes256_gcm_rtpsize` when the server offers it

### 4. Playback worker

Owns one active track pipeline:

- call `GetSong(video_id)`
- filter and choose the allowed adaptive format
- call `Decipher(signature_cipher)`
- open the playable URL over HTTP
- incrementally parse the WebM stream
- extract Opus packets
- push ready-to-send frames into a bounded queue

### 5. RTP pacer

Drains the queue at a strict 20 ms cadence and hands framed Opus payloads to the voice transport. This component is intentionally separate from the upstream reader/demuxer so minor HTTP jitter does not disturb Discord send timing.

## Session State Model

The supervisor owns one authoritative session state machine:

- `Idle`
- `ConnectingVoice`
- `VoiceReady`
- `ResolvingTrack`
- `Buffering`
- `Playing`
- `Paused`
- `Stopping`
- `Error`

State behavior:

- `JoinVoice` moves the service from `Idle` or an old voice session into `ConnectingVoice`
- successful voice setup moves the session into `VoiceReady`
- `Play(videoId)` enters `ResolvingTrack`, then `Buffering`, then `Playing`
- `Pause` moves `Playing` to `Paused`
- `Resume` moves `Paused` back to `Playing`
- `Stop` ends track playback but keeps the voice session connected, returning to `VoiceReady`
- `LeaveVoice` tears down transport and returns to `Idle`
- session-level transport failure transitions to `Error` and requires a fresh `JoinVoice`

## gRPC API Design

Use gRPC for consistency with `ytmusic-service`.

### Unary control RPCs

- `JoinVoice(JoinVoiceRequest) -> JoinVoiceResponse`
- `Play(PlayRequest) -> PlayResponse`
- `Pause(PauseRequest) -> PauseResponse`
- `Resume(ResumeRequest) -> ResumeResponse`
- `Stop(StopRequest) -> StopResponse`
- `LeaveVoice(LeaveVoiceRequest) -> LeaveVoiceResponse`
- `GetState(GetStateRequest) -> SessionStateSnapshot`

### Server-streamed RPC

- `SubscribeEvents(SubscribeEventsRequest) -> stream SessionEvent`

### Join request payload

The join RPC should carry the bot-forwarded voice context needed for the voice connection:

- guild ID
- channel ID
- session ID
- endpoint
- token

### Event stream payloads

The event stream should cover both lifecycle and operational feedback. Example event kinds:

- `voice_connecting`
- `voice_ready`
- `track_resolving`
- `buffering`
- `playing`
- `paused`
- `stopped`
- `track_ended`
- `playback_interrupted`
- `recoverable_warning`
- `fatal_error`

Event payloads should include enough context for the bot to correlate state changes:

- guild ID
- channel ID
- current `videoId`, if any
- selected itag, if any
- human-readable message
- machine-readable error code, when relevant

## Playback Format Selection

The format selector is deterministic and deliberately narrow.

Allowed candidates:

- `audio/webm; codecs="opus"` only
- audio-only formats only
- Discord Voice-compatible Opus stream characteristics, targeting 48 kHz stereo passthrough

Rejected candidates:

- all `audio/mp4` and AAC variants
- all `video/*` formats
- any source requiring decode/re-encode to play

Selection priority:

1. `itag 250`
2. `itag 249`
3. any lower-bitrate WebM/Opus fallback below those

The selector must not choose a higher-quality or higher-bitrate upgrade when `250` is absent. If there is no acceptable candidate in the allowed priority range, the service fails playback as `unsupported_format`.

## Playback Pipeline

For `Play(videoId)`, the service performs the following sequence:

1. verify that the voice transport is ready
2. call `GetSong(videoId)`
3. inspect `adaptive_formats`
4. filter to allowed WebM/Opus candidates
5. choose the format by the selection policy above
6. call `Decipher(signature_cipher)` on the chosen format
7. open the playable URL as an HTTP stream
8. incrementally parse WebM clusters and extract Opus packets
9. fill a bounded queue until the startup threshold is reached
10. start paced RTP send at 20 ms per frame

If a new `Play` arrives while another track is active, the supervisor stops the old playback worker and starts a fresh pipeline for the new `videoId`.

## Buffering Model

This service is not a zero-buffer relay. It uses a small bounded prebuffer.

### Why

Minor jitter in the upstream HTTP stream should not directly produce gaps or irregular pacing in Discord playback.

### Rules

- maintain a startup threshold before sending the first audio frame
- maintain a higher hard cap for bounded memory use
- pause upstream refill when the queue reaches the hard cap
- pace downstream transmission strictly at 20 ms intervals
- on `Pause`, stop draining the queue and suspend refill work for simplicity
- on `Resume`, continue from buffered state or refill first if the queue dropped below a safe threshold

### Design intent

The buffer must be large enough to smooth normal upstream jitter but small enough to preserve low memory use and quick stop/pause responsiveness.

The exact thresholds can be tuned during implementation, but the design target is "a few seconds" of queued Opus frames, not tens of seconds and not full-track buffering.

## Error Handling Policy

### Playback-level failures

These fail the track but keep the voice session alive:

- no acceptable WebM/Opus candidate
- `Decipher` failure
- HTTP open failure
- demux failure before playback starts
- startup buffer never reaches threshold

Expected behavior:

- return an RPC error or failure response for the command in flight
- emit a corresponding `fatal_error` or track-scoped failure event
- remain connected to voice in `VoiceReady`

### Mid-playback upstream failures

If the queue drains because the upstream stream stalls or ends unexpectedly:

- stop track playback cleanly
- send five Opus silence frames before ending transmission
- emit `playback_interrupted` or `track_ended`, depending on cause
- remain voice-connected

### Voice transport failures

If the Discord voice websocket, UDP connection, protocol negotiation, encryption state, or DAVE flow fails:

- transition the session to `Error`
- emit a session-scoped `fatal_error`
- require a fresh `JoinVoice` request from the bot before future playback

## Discord Voice Protocol Notes

The service must implement current Discord voice requirements rather than relying on outdated behavior.

### Core transport

- voice websocket version 8
- websocket heartbeat with `seq_ack`
- UDP IP discovery and `Select Protocol`
- RTP packet construction with Opus payloads
- stereo, 48 kHz Opus frames

### Encryption

- support `aead_xchacha20_poly1305_rtpsize`
- prefer `aead_aes256_gcm_rtpsize` when offered
- use the session description key material and nonce rules required by the chosen mode

### DAVE

The transport design must include DAVE-capable flow for current Discord voice compatibility. This is a hard v1 requirement because the current date is after the March 1, 2026 requirement date described in Discord's documentation.

The implementation plan should break DAVE handling into its own component boundary so that the rest of the playback pipeline does not become entangled with protocol transition logic.

## Testing Strategy

Most correctness should be testable without live Discord.

### Unit tests

- format selection
- supervisor state transitions
- RTP sequence and timestamp progression
- queue and pacing edge cases
- event fanout behavior

### Fixture-driven media tests

- WebM/Opus demux from captured sample streams
- packet extraction ordering
- buffer fill and drain expectations

### Integration tests

- mocked `ytmusic-service` for `GetSong` and `Decipher`
- supported and unsupported format cases
- decipher failure handling
- startup buffering success and failure

### Voice transport tests

- fake voice gateway for hello, ready, select protocol, session description, resume, and failure paths
- local UDP peer for RTP packet validation
- DAVE negotiation and transition handling under controlled fixtures

### Manual validation

At least one real Discord smoke test is still required before considering playback production-ready.

## Observability

Use structured Rust logging suitable for containers.

Recommendations:

- `tracing` with `tracing-subscriber`
- env-filter driven log level control
- optional JSON log formatting for container environments

Key fields to log:

- guild ID
- channel ID
- `videoId`
- selected itag
- current state
- queue depth
- Discord voice endpoint
- relevant error code

The service should also expose standard gRPC health for process-level health. Session readiness and playback readiness belong in `GetState` and `SubscribeEvents`, not solely in the health probe.

## Deployment Model

The service is intended to run beside `ytmusic-service` and the main bot in Podman.

Deployment assumptions:

- same Podman network as the bot and `ytmusic-service`
- no external helper binaries in the container image
- container image should stay Rust-only and small
- service configured with the internal address of `ytmusic-service`
- bot sends voice context through gRPC rather than sharing full Discord gateway responsibility

The service should expose only its control/admin interface and any health endpoint needed for orchestration.

## Recommended Implementation Boundaries

To keep the codebase understandable and testable, split the implementation into a few focused modules:

- `api`: gRPC handlers and proto integration
- `session`: supervisor, state machine, event fanout
- `discord_voice`: websocket, UDP, RTP, encryption, DAVE
- `ytmusic`: gRPC client and format selection logic
- `media`: WebM incremental demux and Opus frame extraction
- `playback`: queue, pacer, track worker
- `config`: environment-driven runtime configuration

Each module should have a single clear responsibility and stable internal interfaces so playback and transport can evolve independently.

## Summary

This v1 design intentionally keeps the product narrow:

- one guild
- one active voice session
- bot-assisted join
- gRPC control
- direct WebM/Opus passthrough only
- bounded prebuffer
- stable 20 ms Discord pacing
- DAVE-capable current Discord voice transport

That scope is small enough to build and validate, while still replacing heavier Discord audio stacks with a memory-efficient Rust service.
