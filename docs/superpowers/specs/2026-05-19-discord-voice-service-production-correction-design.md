# discord-voice-service production correction design

Date: 2026-05-19

## Purpose

This document defines the corrective production specification for `discord-voice-service`.

The existing repository already contains useful pieces:

- a stable gRPC control contract
- real `ytmusic-service` RPC integration for `GetSong` and `Decipher`
- a real WebM/Opus fetch and demux path
- single-session state and event plumbing
- a fake-peer protocol test layer
- container and release packaging

However, the current implementation is not yet production-usable against real Discord voice infrastructure. The main gap is not the YouTube Music path; it is the Discord transport path and the evidence required to prove it works.

This corrective spec keeps the parts that are already sound and replaces the non-compliant transport and validation behavior with explicit, implementable requirements.

## Confirmed constraints

- Single guild only
- One voice session per process
- Main bot boundary remains bot-assisted
- Queue management stays outside `discord-voice-service`
- Playback remains Opus passthrough only
- No decode/transcode fallback
- `SubscribeEvents` remains the authoritative lifecycle stream
- `GetState` remains a resync and debug path only
- `ytmusic-service` remains the source of `GetSong` and `Decipher`
- Live validation must use a real `ytmusic-service` started with `./browser.json`
- Both fake-peer CI and live Discord staging validation are mandatory release gates

## What this spec keeps

This spec does not rewrite the whole service.

The following design decisions remain valid and should be preserved:

- the gRPC API surface in `proto/discordvoice/v1/control.proto`
- the single-session, single-guild process model
- the `videoId`-driven `Play` contract
- the YouTube Music selection policy:
  - only `audio/webm; codecs="opus"`
  - 48 kHz stereo only
  - prefer `itag 250`
  - otherwise `itag 249`
  - otherwise lower-quality Opus WebM fallback only
- bounded prebuffering between upstream fetch and RTP send
- bot ownership of high-level playback decisions
- service ownership of playback execution and transport behavior

## What this spec corrects

This spec corrects the unfinished or non-compliant areas:

- real Discord voice handshake and session establishment
- real forwarded voice-context handling
- removal of synthetic endpoint assumptions
- real transport encryption in the live runtime path
- real DAVE/E2EE use in the live runtime path
- real 20 ms paced packet emission
- transport-level stop behavior
- staging validation with a real Discord bot and real voice channel
- compliance-driven acceptance criteria

## Scope

This version includes:

- a real Discord voice transport implementation that works with authentic forwarded Discord voice context
- a corrected fake-peer test harness that mirrors real Discord semantics closely enough to catch protocol drift
- a staging-only live validation controller binary in this repository
- `twilight`-based Discord gateway and HTTP handling inside that controller
- mandatory real Discord staging validation against a dedicated test guild
- explicit release gates and evidence requirements

This version does not include:

- a full end-user Discord music bot
- queue progression inside `discord-voice-service`
- multi-guild concurrency
- transcoding
- a general-purpose admin plane beyond what is required to validate and operate the service

## Architecture

The service remains a single long-lived voice session runtime with four bounded areas:

- `Control/API`
  - gRPC commands
  - `SubscribeEvents`
  - `GetState`
- `Playback Pipeline`
  - `ytmusic-service` calls
  - stream selection
  - deciphered playable URL resolution
  - HTTP fetch
  - WebM/Opus demux
  - bounded prebuffer
- `Voice Transport`
  - Discord voice WebSocket
  - UDP discovery
  - transport setup
  - RTP send
  - transport encryption
  - DAVE/E2EE handling
  - speaking and stop behavior
- `Recovery Coordinator`
  - upstream stream reopen
  - re-resolution through `ytmusic-service`
  - voice-context refresh handling
  - controlled reconnect behavior

The main bot boundary does not change:

- controller or future main bot decides when to join and what `videoId` to play
- `discord-voice-service` decides how to establish voice, stream media, recover from interruption, and report results

## Runtime state model

The runtime uses this operational state model:

`Idle -> VoiceConnecting -> VoiceReady -> TrackResolving -> Buffering -> Playing -> Recovering -> ReconnectingVoice -> Paused -> Stopping`

State meanings:

- `Idle`
  - no active voice transport
- `VoiceConnecting`
  - real Discord voice session establishment is in progress
- `VoiceReady`
  - the runtime has a usable voice transport and can accept playback
- `TrackResolving`
  - `GetSong` and `Decipher` are active
- `Buffering`
  - fetch and demux are filling the playback queue before send
- `Playing`
  - the pacer is draining frames at a fixed 20 ms cadence
- `Recovering`
  - the runtime is reopening the current source or re-resolving it
- `ReconnectingVoice`
  - the runtime is applying refreshed voice context through a controlled reconnect
- `Paused`
  - packet emission is halted while playback continuity state is retained
- `Stopping`
  - packet emission is ending and the runtime is returning to `VoiceReady` or `Idle`

Error conditions should be reported through events and `GetState`, not by collapsing the process into a dead global state unless the runtime truly cannot continue.

## Discord transport requirements

### Forwarded voice context

The bot-assisted boundary remains:

- the external controller or future main bot must forward:
  - `guild_id`
  - `channel_id`
  - `session_id`
  - `endpoint`
  - `token`

The service must treat these fields as authentic Discord voice context.

The implementation must not require:

- custom query parameters appended to the endpoint
- synthetic `udp` or `ssrc` fields inside the endpoint
- fabricated session description data injected by the test harness

If the implementation only works with a custom endpoint shape, it is non-compliant.

### Voice gateway version and setup

The voice WebSocket must connect using Discord voice gateway version `8`.

The implementation must:

- normalize the voice gateway URL to use `?v=8&encoding=json`
- use the authentic forwarded endpoint as the source of the voice host
- perform the real Discord voice setup flow rather than a test-only shortcut

The service must not declare `VoiceReady` until real voice-session establishment has completed.

### Real session establishment

The runtime must implement the actual Discord voice setup sequence required for a bot-assisted session:

1. connect to the voice WebSocket for the forwarded endpoint
2. identify or resume using the forwarded `guild_id`, `session_id`, and `token`
3. consume server voice-session data required to proceed
4. perform UDP IP discovery against the real voice server
5. choose a supported transport encryption mode from the server-reported list
6. send `Select Protocol` using the discovered address, discovered port, and selected mode
7. consume `Session Description`
8. establish the transport packet-protection context required for live packet send
9. establish the DAVE/E2EE context required for live media send when Discord requires it
10. only then transition to `VoiceReady`

This service may use helper structs and modules internally, but the production path must execute a real Discord-compatible setup sequence end to end.

### UDP behavior

The runtime must:

- send UDP discovery packets to the real Discord voice server
- receive the corresponding UDP reply
- extract the discovered local IP and port
- keep the discovered address available for transport setup and reconnect handling

The deployment environment for staging and production must allow inbound UDP replies from Discord voice servers.

### Voice resume and `seq_ack`

The implementation must:

- track the last sequence-numbered voice gateway event seen
- include `seq_ack` in voice heartbeats
- include `seq_ack` in voice resume payloads
- attempt resume when the voice session can be resumed
- fall back to a full reconnect when resume fails

### Speaking and stop behavior

Before the first transmitted audio packet, the runtime must send at least one Opcode 5 `Speaking` payload.

When stopping active transmission, the runtime must send exactly five Opus silence frames before ending packet flow.

These rules are mandatory in the live path, not just in isolated tests.

### Packet pacing

The runtime must emit audio at a stable 20 ms cadence.

Requirements:

- the playback pipeline may fetch and demux ahead into a bounded queue
- the pacer must be the only component allowed to drive live packet send timing
- the runtime must not drain the queue as fast as possible
- event timestamps and playback position accounting must follow paced emission, not buffered availability

### Transport encryption

The live runtime path must apply real Discord transport packet protection.

Requirements:

- support `aead_xchacha20_poly1305_rtpsize`
- prefer `aead_aes256_gcm_rtpsize` when the server reports it as available
- do not rely on deprecated transport encryption modes
- send the chosen mode in `Select Protocol`
- protect transmitted media packets in the live send path

It is not sufficient to define mode-selection helpers without applying the chosen mode to live packets.

### DAVE and E2EE

DAVE support is mandatory for the production path.

Requirements:

- the implementation must use `libdave` or a functionally equivalent complete implementation strategy
- the live voice runtime must actually invoke DAVE/E2EE session setup where required by Discord
- the runtime must handle the Discord voice opcodes and transitions required for supported DAVE protocol versions
- the runtime must support DAVE re-establishment when voice context changes require reconnect behavior

It is not sufficient to:

- vendor `libdave`
- compile FFI bindings
- pass isolated handshake unit tests

The live voice runtime path must use the DAVE session machinery as part of real packet transmission readiness.

## Playback pipeline requirements

The playback path remains:

`Play(videoId)` -> `GetSong` -> format filtering -> format selection -> `Decipher(signatureCipher)` -> playable URL open -> incremental HTTP fetch -> WebM/Opus demux -> bounded prebuffer -> paced 20 ms packet emission

Selection policy:

- only `audio/webm; codecs="opus"`
- only 48 kHz stereo
- prefer `itag 250`
- then `itag 249`
- then lower-quality Opus WebM fallback only
- reject `audio/mp4`
- reject all `video/*`
- reject higher-quality upgrades above the preferred low-bitrate passthrough path

The runtime must continue to use real `ytmusic-service` calls for `GetSong` and `Decipher`.

The live validation path is invalid if it:

- bypasses `ytmusic-service`
- fabricates a playable URL
- substitutes a fake metadata path

## Recovery and voice-context refresh

### Upstream media recovery

When the upstream stream stalls or the playable URL becomes stale, the runtime must:

1. attempt one reopen of the current playable source using the last emitted playback position
2. if reopen fails because the source is stale, inaccessible, or range recovery cannot satisfy the requested position, re-resolve through `ytmusic-service`
3. attempt one reopen using the newly resolved playable URL
4. resume from the first demuxed packet whose end timestamp is strictly greater than the last emitted playback position

The runtime must emit a deterministic playback interruption failure if those recovery steps do not restore playback.

### Voice-context refresh

`UpdateVoiceContext` remains the path for forwarded `session_id`, `endpoint`, `token`, and channel updates after the initial join.

The runtime must interpret this as a controlled transport refresh:

- emit `VoiceReconnecting`
- attempt a real reconnect using the new authentic forwarded voice context
- preserve current track context when feasible
- continue playback from the first demuxed packet whose end timestamp is strictly greater than the last emitted playback position if reconnect succeeds
- emit deterministic failure if reconnect does not succeed

The runtime must not claim support for in-place session migration if it is actually doing a fresh underlying reconnect.

## Fake-peer CI requirements

The fake-peer layer remains mandatory, but its purpose is limited.

It must verify:

- control RPC behavior
- event-stream behavior
- readiness and invalid-state behavior
- `ytmusic-service` interaction where fake service responses are sufficient
- WebM/Opus fetch, buffering, reopen, and re-resolution logic
- RTP sequencing and timestamp progression
- 20 ms pacing behavior
- speaking-before-audio behavior
- five-silence-frame stop behavior
- recovery state handling
- voice-context refresh state handling

The fake-peer harness must not:

- invent a custom endpoint contract that the production service depends on
- inject custom `udp` or `ssrc` query parameters as a requirement for successful connection
- bypass real transport-setup phases that the production code claims to support

The fake-peer harness may simulate Discord responses, but those responses must align with the real protocol shape.

## Live Discord staging validation

### Controller location and role

A staging-only controller binary must live inside this repository.

This controller is not the future main bot. It exists only to prove the bot-assisted boundary against real Discord.

It must use `twilight` for Discord integration.

Responsibilities:

- load staging configuration and secrets
- connect to the real Discord gateway
- request the bot’s join into the configured test voice channel
- wait for authentic `VOICE_STATE_UPDATE` and `VOICE_SERVER_UPDATE`
- assemble forwarded voice context
- call the gRPC control surface on `discord-voice-service`
- subscribe to `SubscribeEvents`
- drive the join-forward-play-assert flow
- exit non-zero on failure

Non-responsibilities:

- no end-user commands
- no queue behavior
- no music search or recommendation logic
- no fake Discord shortcuts

### Staging inputs

The staging controller must read the following environment variables:

- `APPLICATION_ID`
- `BOT_TOKEN`
- `TEST_GUILD_ID`
- `TEST_VOICE_CHANNEL_ID`
- `TEST_VIDEO_ID`
- `DISCORD_VOICE_SERVICE_ADDR`
- `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR`

The live validation workflow may use a local `.env` file for operator convenience, but the file must remain gitignored and must not be required in CI.

### `ytmusic-service` requirement

During live validation, `ytmusic-service` must be started using `./browser.json`.

The staging run is invalid if the live test path does not use that configuration input.

### Controller join behavior

The controller must use `twilight` to perform the actual bot-side join flow for the configured guild and voice channel.

The controller must wait until it has both:

- the bot’s relevant `VOICE_STATE_UPDATE`
- the matching `VOICE_SERVER_UPDATE`

Only then may it forward the assembled voice context into `discord-voice-service`.

### Minimum live assertions

A passing live validation run must prove all of the following:

1. the controller successfully connects to the real Discord gateway
2. the bot successfully initiates a join into `TEST_VOICE_CHANNEL_ID`
3. authentic forwarded voice context is acquired from real gateway events
4. `discord-voice-service` accepts that context and establishes a real voice session
5. `Play(TEST_VIDEO_ID)` succeeds
6. the event stream reaches the expected lifecycle without `FatalError`
7. media transmission begins and continues for a minimum bounded interval against real Discord infrastructure
8. cleanup succeeds and the bot leaves voice

For this spec, the minimum bounded interval is:

- at least 5 consecutive seconds of paced media transmission after the first confirmed transmitted audio packet
- with no `FatalError`
- with no unexpected transition out of `Playing` before the interval completes

### Evidence requirement

The live validation run must produce enough evidence to be auditable:

- start and end timestamps
- commit SHA under test
- whether join succeeded
- whether voice context was acquired
- whether `VoiceReady` was reached
- whether `Playing` was reached
- whether media transmission was sustained for the minimum interval
- whether cleanup succeeded
- explicit failure reason when the run fails

The controller should emit this as structured logs and exit status. It does not need to create a separate persistent database or reporting service.

## Release gates

No build may be called production-ready unless both of the following pass for the candidate commit:

1. fake-peer CI passes
2. live Discord staging validation passes

These are different gates with different purposes.

Recommended enforcement model:

- fake-peer CI runs on ordinary branch and pull request validation
- live Discord staging validation runs on a protected staging or release workflow using real secrets
- release or protected merge is blocked when the live validation gate has not passed for the candidate commit

This keeps the implementation practical while preserving the strict requirement that real Discord validation is mandatory before the service is declared production-ready.

## Explicit non-compliance conditions

An implementation is non-compliant with this spec if any of the following are true:

- it only works when the voice endpoint contains synthetic `udp` or `ssrc` query parameters
- it sends plain RTP payloads without applying the selected Discord transport protection in the live path
- DAVE exists only as a compiled dependency or isolated test and is not exercised by the live runtime path
- playback send timing is “as fast as possible” rather than paced at 20 ms intervals
- the fake-peer harness hides production shortcuts that real Discord does not provide
- the live test bypasses `ytmusic-service`
- the live test does not start `ytmusic-service` with `./browser.json`
- the live test does not use a real bot token and real voice channel
- the service is called production-ready without both release gates passing

## Implementation order

To keep this spec implementable, the expected execution order is:

1. correct the fake-peer harness so it no longer depends on synthetic endpoint assumptions
2. implement the real Discord voice handshake and session-establishment path
3. wire transport encryption into the live packet send path
4. wire DAVE/E2EE into the live runtime path
5. wire the real pacer into playback emission
6. complete stop and reconnect semantics in the live runtime
7. add the `twilight` staging controller
8. add the live staging workflow and operator documentation
9. enforce both release gates

## Acceptance criteria

This corrective version is complete only when all of the following are true:

- authentic forwarded voice context from real Discord can establish a usable voice session
- the service no longer depends on synthetic endpoint query parameters
- the live runtime applies real transport protection
- the live runtime uses DAVE/E2EE where Discord requires it
- the live runtime sends Opus at a stable 20 ms cadence
- speaking-before-audio and five-silence-frame stop behavior are enforced in the live path
- `ytmusic-service` remains the real source for `GetSong` and `Decipher`
- the fake-peer harness still passes while mirroring the real protocol shape closely enough to catch drift
- the `twilight` staging controller can join, forward context, play `TEST_VIDEO_ID`, and verify a successful real session
- both fake-peer CI and live Discord staging validation pass for the candidate commit
