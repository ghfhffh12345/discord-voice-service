# discord-voice-service production design

Date: 2026-05-18

## Purpose

This document defines the next version of `discord-voice-service` as a production-usable single-guild Discord Voice microservice.

The current repository already contains:

- a gRPC control contract
- startup configuration and health checks
- a basic single-session state model
- container and release packaging
- a narrow YouTube Music Opus format-selection policy

It does not yet contain a real end-to-end voice transport or playback path. The goal of this version is to complete the real Discord voice session, `ytmusic-service` integration, WebM/Opus passthrough pipeline, and recovery behavior so the service can be used in production.

## Confirmed constraints

- Single guild only
- One voice session per process
- Main bot remains the owner of user-facing commands, queue policy, radio logic, and next-track decisions
- `discord-voice-service` owns the full playback and voice transport lifecycle after the bot issues `Play(videoId)`
- Bot-assisted Discord voice boundary remains in place
- The main bot continues forwarding Discord voice session updates after initial join
- Playback remains Opus passthrough only
- No decode/transcode fallback path is added in this version
- `SubscribeEvents` is the authoritative playback lifecycle stream
- `GetState` remains a resync/debug path only
- Recovery target is strong recovery:
  - reopen the playable stream when possible
  - re-resolve through `ytmusic-service` when needed
  - continue from a close playback position if recovery succeeds
- Queue management stays in the main bot service

## Scope

This version includes:

- real `ytmusic-service` RPC integration
- real Discord voice transport implementation
- mandatory DAVE/E2EE-compatible voice session handling
- real WebM/Opus demux and bounded prebuffering
- paced Discord RTP send
- authoritative event streaming
- internal voice-context rollover handling
- production-oriented recovery behavior
- baseline observability needed to operate the service safely

This version does not include:

- queue ownership inside `discord-voice-service`
- multi-guild concurrency
- general audio transcoding
- AAC playback support
- `audio/mp4` fallback
- broader admin tooling beyond what is needed for production operation

## Architecture

The service stays single-session and single-guild, but the runtime becomes a real long-lived playback system instead of a state-only scaffold.

The core structure is:

- `Control/API`
  - exposes gRPC commands
  - exposes `SubscribeEvents`
  - exposes `GetState`
- `Voice Transport`
  - owns Discord voice WebSocket communication
  - owns UDP discovery and media transport
  - owns transport encryption mode negotiation and packet protection
  - owns DAVE/E2EE negotiation, transitions, and re-establishment
  - owns speaking state, silence termination behavior, and paced RTP send output
- `Playback Pipeline`
  - owns `ytmusic-service` RPC interaction
  - owns format filtering and selection
  - owns deciphered playable URL resolution
  - owns HTTP fetch, WebM demux, bounded Opus buffering, and frame delivery to the pacer
- `Recovery Coordinator`
  - owns upstream interruption recovery
  - owns voice transport rollover recovery
  - owns re-resolution and near-position continuation behavior

The service boundary remains:

- bot decides what to play
- `discord-voice-service` decides how to play it reliably

Once the bot sends `Play(videoId)`, this service owns resolve, prebuffer, send, recover, stop, and event reporting for that track.

## Discord protocol requirements

The production implementation must follow the latest official Discord voice requirements as documented on 2026-05-18.

### Voice gateway version

- the voice WebSocket must connect with `?v=8`
- versions below 4 are not allowed
- the implementation should treat version 8 as mandatory rather than optional because buffered resume behavior is part of the recovery design for this service

### UDP transport

- Discord voice uses a separate UDP transport for media
- the service must be able to both send and receive UDP packets
- the deployment model must allow inbound UDP replies from Discord voice servers through container, firewall, NAT, and Podman networking
- the implementation must perform Discord IP discovery and keep the discovered local UDP address available for protocol selection and reconnect flows

### Voice resume and sequence acknowledgement

- because the design depends on recovery and transport rollover, voice gateway resume support is required
- when using voice gateway version 8, the service must track the last sequence-numbered gateway message received
- the service must include `seq_ack` in:
  - Opcode 3 Heartbeat payloads
  - Opcode 7 Resume payloads
- if resume fails, the implementation must fall back to a full reconnect path

### Speaking and stop behavior

- the service must send at least one Opcode 5 Speaking payload before sending any audio packets
- the service must use a non-zero speaking mode when transmitting audio
- the `delay` field should be `0` for bot usage
- before stopping transmission, the service must send five Opus silence frames (`0xF8, 0xFF, 0xFE`) to avoid interpolation artifacts in Discord playback

These behaviors are mandatory protocol requirements, not optional polish.

### Transport encryption modes

- the service must support `aead_xchacha20_poly1305_rtpsize`
- the service should prefer `aead_aes256_gcm_rtpsize` when the voice server reports it as available
- the implementation must not rely on deprecated transport encryption modes
- the selected transport encryption mode must be sent in Opcode 1 Select Protocol

### DAVE / E2EE

- DAVE support is required for this version of the service
- the implementation must treat DAVE as first-class protocol scope, not as an optional enhancement
- the implementation must support the voice gateway opcodes and transitions required for the currently supported DAVE protocol versions
- because Discord documents some DAVE opcodes as binary WebSocket messages and points to the DAVE whitepaper for exact payload formats, the implementation should use `libdave` or a functionally equivalent implementation strategy rather than attempting an ad hoc partial implementation
- the implementation must support:
  - advertising supported DAVE protocol version in Identify
  - consuming `dave_protocol_version` from Session Description
  - DAVE protocol transition readiness and execution
  - MLS group membership changes
  - external sender package handling as required by Discord's MLS group model

The implementation plan should assume that transport encryption and DAVE frame-level encryption both exist and both must be handled correctly.

## Runtime state model

The session runtime uses a concrete operational state machine:

`Idle -> VoiceConnecting -> VoiceReady -> TrackResolving -> Buffering -> Playing -> Recovering -> ReconnectingVoice -> Paused -> Stopping`

Error conditions should be reported as events and reflected in snapshots. The service should avoid collapsing into an unrecoverable global error state unless the runtime genuinely cannot proceed.

Each state maps to active work and cancellation semantics:

- `VoiceConnecting`
  - voice WebSocket and UDP setup in progress
- `VoiceReady`
  - transport is established and ready for playback commands
- `TrackResolving`
  - `GetSong` and `Decipher` work is active
- `Buffering`
  - HTTP fetch and demux are filling the Opus queue
- `Playing`
  - pacer is draining queued Opus frames to Discord RTP
- `Recovering`
  - stream reopen, re-resolution, and playback continuation logic is active
- `ReconnectingVoice`
  - updated voice context is being applied through an internal transport rollover
- `Paused`
  - playback position is retained, transport remains valid, packet emission is paused
- `Stopping`
  - playback tasks are being cancelled and the session is returning to `VoiceReady` or `Idle`

## gRPC control model

The production control surface remains intentionally small.

RPCs:

- `JoinVoice`
- `UpdateVoiceContext`
- `Play`
- `Pause`
- `Resume`
- `Stop`
- `LeaveVoice`
- `GetState`
- `SubscribeEvents`

Behavior:

- `JoinVoice`
  - installs the initial voice context for an idle session
  - should only be considered successful after the service receives enough forwarded context from the bot to begin a voice connection attempt
- `UpdateVoiceContext`
  - applies refreshed `session_id`, `endpoint`, `token`, and channel updates after the initial join
  - is valid both while idle-in-voice and during active playback
  - is the only supported path for forwarded voice token, endpoint, or session refreshes after initial join
- `Play`
  - starts a new playback lifecycle for a single `videoId`
- `Pause`
  - pauses packet emission while preserving playback continuity state
- `Resume`
  - resumes from buffered or recovered playback state
- `Stop`
  - stops the current track and returns the runtime to `VoiceReady`
- `LeaveVoice`
  - tears down the voice session and returns the runtime to `Idle`
- `GetState`
  - exposes a point-in-time operational snapshot
- `SubscribeEvents`
  - exposes the authoritative lifecycle stream for the bot

The bot-to-service contract must also define failure semantics for missing forwarded voice context:

- if the main bot never receives the required Discord `VOICE_STATE_UPDATE` and `VOICE_SERVER_UPDATE` pair, it cannot provide a usable voice context
- the service must therefore expose a deterministic join-timeout or join-failed path instead of waiting indefinitely
- the failure model must account for the documented case where a bot cannot join a full voice channel and therefore receives neither update event unless it bypasses the limit

## Event model

`SubscribeEvents` is the authoritative playback lifecycle stream. The main bot should treat this stream as the primary source of truth for queue progression and recovery awareness.

`GetState` is a secondary path used for:

- startup reconciliation
- resync after event-stream reconnect
- operator and debug inspection

The production event set includes at least:

- `voice-connecting`
- `join-timeout`
- `join-failed`
- `voice-ready`
- `track-resolving`
- `buffering`
- `track-started`
- `paused`
- `resumed`
- `recovering`
- `recovered`
- `track-ended`
- `playback-interrupted`
- `voice-reconnecting`
- `voice-reconnected`
- `fatal-error`

Each event should include:

- stable session identifier
- guild identifier
- channel identifier
- current `video_id` when present
- selected `itag` when known
- playback position in milliseconds when meaningful
- machine-readable reason or error code
- optional operator-facing message text

Relevant machine-readable reasons should include at least:

- channel full / no forwarded voice context
- invalid or expired voice token
- voice resume failed
- DAVE transition failed
- unsupported encryption mode
- UDP discovery failed
- upstream playback URL stale
- playback source unsupported

`GetState` should expose at least:

- runtime state
- guild and channel identifiers
- current `video_id`
- selected `itag`
- queue depth
- current playback position estimate
- whether recovery is in progress
- whether voice transport rollover is in progress
- most recent recovery reason or failure reason when applicable

## Voice context update semantics

Discord does not support strict in-place reuse of the previous voice session across all channel changes and token updates. For this version, "update in place" means seamless service-level handling, not literal protocol-level session reuse.

When the bot forwards refreshed voice context during playback:

- `discord-voice-service` accepts the updated context
- it performs an internal controlled transport rollover
- it re-establishes the Discord voice transport as needed
- it attempts to continue the current track without requiring a second `Play` from the bot

Expected lifecycle:

- emit `voice-reconnecting`
- rebuild transport
- preserve or restore playback continuity from the closest practical point
- emit `voice-reconnected` if successful
- emit `fatal-error` if the rollover fails and the session cannot continue

From the bot's perspective, the playback session remains logically continuous even if the service has to reconnect internally.

## Playback pipeline

The playback path is:

`Play(videoId)` -> `GetSong` -> format filtering and selection -> `Decipher(signatureCipher)` -> playable URL open -> incremental HTTP read -> WebM/Opus demux -> bounded Opus prebuffer -> paced 20 ms RTP send

Discord transport requirements for the media path:

- outgoing voice must be Opus
- audio must be stereo
- audio must be 48 kHz
- media packets must be sent in RTP with Discord-compatible sequence and timestamp progression
- the service must send Speaking before first packet emission
- the service must terminate active transmission with five silence frames before stopping

The service calls `ytmusic-service` internally for:

- `GetSong`
- `Decipher`

Format filtering rules:

- only `audio/webm; codecs="opus"` candidates are considered
- audio must be 48 kHz stereo
- preferred order:
  - `itag 250`
  - `itag 249`
  - another lower-bitrate WebM/Opus fallback below that range
- no `audio/mp4`
- no AAC
- no `video/*`
- no higher-quality upgrade above the preferred low-bitrate path

If no valid passthrough source exists, playback fails clearly as unsupported.

## Buffering and pacing

Playback startup uses a hybrid buffering policy:

- fixed default startup target under normal conditions
- bounded adaptive bump under unstable fetch conditions
- bounded adaptive bump during recovery

This balances startup latency against resilience to upstream jitter.

Operationally:

- the fetch and demux side reads ahead from the playable URL
- the bounded Opus queue decouples upstream timing from Discord send timing
- the pacer emits RTP payloads at a stable 20 ms cadence
- the transport layer applies the selected Discord transport encryption mode to each packet
- when DAVE is active, the media path must also apply the correct frame-level E2EE context before packet protection

The service should preserve low and predictable memory use by bounding the queue. Recovery and startup policy may temporarily increase the target within configured limits, but the queue remains bounded.

## Recovery model

Recovery is first-class and layered.

### Upstream interruption recovery

If the upstream HTTP stream stalls or the buffer drains unexpectedly:

1. attempt to reopen the same playable stream
2. if that fails or the playable URL is stale, re-run `GetSong`
3. re-run `Decipher`
4. reopen the stream from the closest practical playback point
5. resume playback with tight continuation if recovery succeeds

If recovery fails, emit `playback-interrupted` or `fatal-error` depending on whether the session remains viable for future commands.

### Voice transport recovery

If voice transport must be refreshed because the bot forwards updated voice context:

1. emit `voice-reconnecting`
2. create a new transport from the new context
3. preserve the playback pipeline if possible
4. if packet emission is interrupted, continue from buffered or recovered position
5. emit `voice-reconnected` on success

If the transport WebSocket is severed without a new forwarded voice context:

1. attempt voice gateway resume using version 8 semantics and `seq_ack`
2. if resume succeeds, continue the active transport lifecycle
3. if resume fails, reconnect using the latest valid forwarded voice context
4. if reconnect cannot be completed, emit a deterministic failure event

### Tight continuation target

Resumed playback should be close to the prior playback point, allowing only a small sub-second discontinuity when recovery succeeds.

This does not require exact sample-perfect seeking. It does require:

- deterministic playback position accounting from sent Opus frame duration
- a nearest-safe-resume rule for reopened streams
- stream reopen logic that aims for a close continuation point instead of replaying large sections

## Queue ownership

Queue management stays in the main bot service.

Responsibilities remain split as:

- bot owns queue progression, autoplay, radio logic, and next-track policy
- `discord-voice-service` owns execution and lifecycle reporting for the current track

When playback ends normally:

- `discord-voice-service` emits `track-ended`
- the runtime returns to `VoiceReady`
- the bot decides what to play next

## Testing strategy

Testing should be organized around failure boundaries rather than only around helpers.

### Unit tests

Cover:

- format filtering and selection
- playback position accounting
- RTP timestamp and sequence progression
- bounded queue behavior
- state transitions and command guards
- reason-code mapping and event serialization

### Integration tests

Cover:

- gRPC control behavior through a real in-process server
- mocked `ytmusic-service` responses for `GetSong` and `Decipher`
- startup buffering behavior
- upstream interruption recovery
- `UpdateVoiceContext` during playback
- `GetState` reconciliation after event-stream reconnect

### Transport simulation tests

Use a fake Discord voice peer to exercise:

- voice WebSocket negotiation
- UDP discovery
- UDP receive path behavior
- version 8 heartbeat and `seq_ack` handling
- version 8 resume behavior
- encryption mode selection
- DAVE/E2EE handshakes
- DAVE transition execution
- paced packet delivery
- Speaking before audio
- silence frame termination before stop
- transport rollover behavior

### Live verification

Reserve real Discord for:

- smoke tests
- reconnect tests
- soak tests under longer playback sessions

## Observability and production-readiness

This version includes only the operator-facing features needed to run the service safely.

Required observability:

- structured `tracing` logs
- stable event and reason codes
- gRPC health for process liveness
- readiness semantics based on runtime initialization and `ytmusic-service` reachability
- useful `GetState` snapshots for operator inspection
- counters and timings for:
  - join timeouts
  - missing forwarded voice context failures
  - recoveries
  - buffer underruns
  - transport reconnects
  - transport resume attempts
  - transport resume failures
  - UDP discovery failures
  - DAVE transition failures
  - stream reopen attempts
  - re-resolution attempts
  - fatal playback failures

Readiness must not depend on whether a track is currently playing. A ready service may be idle.

## Production acceptance criteria

This version is considered complete when it satisfies all of the following:

- can join voice using forwarded bot context
- can accept subsequent forwarded voice context updates
- can connect to Discord voice with gateway version 8
- can perform UDP discovery and receive UDP traffic required by Discord voice
- can send Speaking before audio and five silence frames before stopping
- can support `aead_xchacha20_poly1305_rtpsize` and prefer `aead_aes256_gcm_rtpsize` when available
- can handle required DAVE/E2EE protocol setup and transitions for a live voice session
- can resume or reconnect voice transport using documented version 8 semantics when feasible
- can call `ytmusic-service` `GetSong` and `Decipher` for real playback startup
- can select a valid low-bitrate Discord-safe Opus source using the agreed priority rules
- can fetch, demux, prebuffer, and send a real YouTube Music Opus stream to Discord end to end
- can emit authoritative lifecycle events through `SubscribeEvents`
- can expose consistent snapshots through `GetState`
- can survive ordinary upstream jitter without audible instability in normal conditions
- can recover automatically from at least some playable-URL or HTTP interruptions
- can perform internal voice transport rollover and continue the active track when Discord voice context changes
- can report deterministic failure causes to the bot when playback or recovery fails

## Implementation guidance

This version should be planned and executed as a focused production-core milestone.

Recommended delivery emphasis:

1. real `ytmusic-service` integration
2. real Discord voice transport and DAVE handling
3. real WebM/Opus demux plus paced RTP send
4. authoritative event streaming
5. strong recovery and voice-context rollover
6. baseline observability required for production operation

Broader admin tooling, multi-guild scaling, and non-passthrough playback should remain out of scope for this milestone.
