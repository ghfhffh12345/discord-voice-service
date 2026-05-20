# Discord Voice Service Production Correction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the unfinished Discord transport path so `discord-voice-service` can establish a real Discord voice session, transmit protected paced Opus audio, and pass both fake-peer CI and live Discord staging validation.

**Architecture:** Keep the existing single-session runtime, `ytmusic-service` integration, and Opus-only media pipeline. Replace the synthetic voice connection path with a real Discord voice setup sequence, wire packet protection and DAVE into the live runtime, add a `twilight` staging controller, and enforce two release gates: fake-peer CI plus live Discord staging validation.

**Tech Stack:** Rust, tonic gRPC, tokio, reqwest, webm-iterable, twilight, tokio-tungstenite, native `libdave` FFI, GitHub Actions, Podman

**Completion Note:** Execution completed. Final verification succeeded, and the final implementation commit is `bdb0cf18` (`build: enforce production release gates`).

---

## Fresh-session handoff notes

Treat this plan as the authoritative execution order for a fresh session.

- Do not redesign the service boundary.
- Do not replace `ytmusic-service`.
- Do not weaken the release gates.
- Do not reintroduce synthetic endpoint query parameters such as `udp=` or `ssrc=` into the production contract.
- Do not declare success from fake-peer CI alone.

Before starting code changes in a fresh session:

1. read [2026-05-19-discord-voice-service-production-correction-design.md](/home/ghfhffh12345/discord-voice-service/docs/superpowers/specs/2026-05-19-discord-voice-service-production-correction-design.md)
2. read this plan end to end
3. inspect current `src/discord_voice/session.rs`, `src/session/runtime.rs`, `src/discord_voice/udp.rs`, and `tests/support/fake_discord.rs`
4. preserve the current `ytmusic-service` integration and selection policy unless a task explicitly changes it

The current known production blockers are:

- the live connection path still assumes a synthetic endpoint shape
- the live send path still emits plain RTP payloads instead of protected media packets
- the live runtime still does not use DAVE session state to make media transmission ready
- the runtime still drains queued audio too aggressively unless the pacer is wired into the send path
- the current green test suite is not sufficient evidence of production readiness by itself

## Current project snapshot

As of this plan:

- the gRPC control contract already exists in `proto/discordvoice/v1/control.proto`
- `src/ytmusic/client.rs` already performs real `GetSong` and `Decipher` RPCs
- `src/playback/worker.rs` and `src/playback/recovery.rs` already perform real HTTP fetch, demux, and reopen/reresolve logic
- `src/discord_voice/dave.rs` already contains native `libdave` wrappers and isolated tests
- `tests/runtime_end_to_end_playback.rs` and `tests/support/fake_discord.rs` currently prove only a fake-peer happy path, not a real Discord-compatible one
- `docs/superpowers` is gitignored in this repo; any intentional documentation change under that tree must be staged with `git add -f`

## Live validation contract

The staging controller and live workflow must use this exact environment variable contract:

- `APPLICATION_ID`
- `BOT_TOKEN`
- `TEST_GUILD_ID`
- `TEST_VOICE_CHANNEL_ID`
- `TEST_VIDEO_ID`
- `DISCORD_VOICE_SERVICE_ADDR`
- `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR`

The live validation path must also obey these runtime inputs:

- `ytmusic-service` must be started with `./browser.json`
- the controller must acquire authentic `VOICE_STATE_UPDATE` and `VOICE_SERVER_UPDATE`
- the controller must forward authentic `guild_id`, `channel_id`, `session_id`, `endpoint`, and `token`
- the controller must call `Play(TEST_VIDEO_ID)` against the real service under test

## Expected live event sequence

For a passing live run, the authoritative event sequence from `SubscribeEvents` must be:

1. `VoiceConnecting`
2. `VoiceReady`
3. `TrackResolving`
4. `Buffering` if startup buffering is observable in the runtime, otherwise direct transition to `Playing`
5. `Playing`
6. zero or more non-fatal informational events that do not leave `Playing`
7. `TrackEnded` after successful paced transmission and normal track completion

Disallowed pass conditions:

- `FatalError` at any point
- `PlaybackInterrupted` before the minimum live interval completes
- `VoiceReconnecting` during the base staging success scenario
- successful process exit without `VoiceReady`
- successful process exit without `Playing`

The live validation run passes only if:

- the controller confirms at least 5 consecutive seconds of paced media transmission after the first confirmed transmitted audio packet
- the event stream remains compatible with the sequence above
- cleanup succeeds and the bot leaves voice

## Runner policy

The implementation should target this enforcement order:

- first-class support: self-hosted staging runner with stable outbound internet and inbound UDP replies
- acceptable secondary path: GitHub-hosted runner for non-secret local checks and workflow structure validation

The live Discord release gate should be treated as self-hosted first unless the implementation proves GitHub-hosted runner networking is reliable enough for real UDP media validation.

## File structure

### Existing files to modify

- `src/discord_voice/session.rs`
  - Replace the synthetic endpoint-query contract with real Discord voice setup orchestration.
- `src/discord_voice/gateway.rs`
  - Expand from heartbeat/resume helpers into a real voice gateway client that can drive setup.
- `src/discord_voice/udp.rs`
  - Apply live packet protection, not just plain RTP send.
- `src/discord_voice/rtp.rs`
  - Keep RTP header construction, but separate it cleanly from packet protection.
- `src/discord_voice/crypto.rs`
  - Keep mode selection and add live packet-protection state and helpers.
- `src/discord_voice/dave.rs`
  - Keep FFI/wrapper layer and expose runtime-usable DAVE session hooks.
- `src/session/runtime.rs`
  - Wire real voice connection, paced send, stop behavior, reconnect behavior, and failure paths.
- `src/playback/pacer.rs`
  - Turn constants into the real pacer used by runtime send.
- `src/playback/worker.rs`
  - Keep buffering logic, but stop it from directly implying send timing.
- `src/api/service.rs`
  - Surface any state or event corrections needed by the stricter runtime behavior.
- `tests/support/fake_discord.rs`
  - Replace the synthetic endpoint shortcut with a fake peer that mirrors real Discord semantics more closely.
- `.github/workflows/*.yml`
  - Add or extend workflows for fake-peer CI and live staging validation.
- `README.md`
  - Document the new staging controller, staging env, and release-gate expectations.

### New files to create

- `src/discord_voice/protocol.rs`
  - Voice opcode payload types and parsing helpers used by the real setup flow.
- `src/discord_voice/protection.rs`
  - Live transport packet-protection context built from session description and selected mode.
- `src/discord_voice/handshake.rs`
  - High-level real voice setup sequence: connect, identify/resume, UDP discovery, select protocol, session description, readiness.
- `src/bin/staging_live_check.rs`
  - `twilight` staging controller binary for join-forward-play-assert flow.
- `tests/voice_handshake.rs`
  - Fake-peer tests for real handshake flow.
- `tests/voice_packet_protection.rs`
  - Tests that live send path applies transport protection.
- `tests/pacer_runtime.rs`
  - Tests proving paced 20 ms emission behavior.
- `tests/live_contract_controller.rs`
  - Narrow controller unit tests for env parsing and validation behavior.
- `.github/workflows/live-staging.yml`
  - Protected staging workflow that uses real Discord secrets and runs the controller.
- `AGENTS.md`
  - Local project context and structure notes for future sessions and agents.

## Task 1: Correct the fake-peer contract

**Files:**
- Create: `tests/voice_handshake.rs`
- Modify: `tests/support/fake_discord.rs`
- Modify: `src/discord_voice/session.rs`
- Test: `tests/runtime_end_to_end_playback.rs`

- [x] **Step 1: Write the failing handshake-shape test**

```rust
#[tokio::test]
async fn connected_voice_session_does_not_require_synthetic_endpoint_query_params() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.is_connected(), "real-shape endpoint should connect");
}
```

- [x] **Step 2: Run the failing test**

Run: `cargo test connected_voice_session_does_not_require_synthetic_endpoint_query_params -v`

Expected: FAIL because `ConnectedVoiceSession::connect` currently returns disconnected unless the endpoint contains custom `udp` and `ssrc` query parameters.

- [x] **Step 3: Rewrite the fake peer to expose a real Discord-like contract**

```rust
pub struct FakeDiscordPeer {
    endpoint_host: String,
    gateway_url: String,
    udp_addr: SocketAddr,
    // ...
}

impl FakeDiscordPeer {
    pub async fn spawn_real_shape() -> Self {
        // Serve a websocket endpoint that looks like a voice host,
        // but keep UDP/session data inside protocol messages instead of endpoint query params.
    }

    pub fn voice_context(&self, guild_id: &str, channel_id: &str, session_id: &str, token: &str) -> VoiceContext {
        VoiceContext {
            guild_id: guild_id.into(),
            channel_id: channel_id.into(),
            session_id: session_id.into(),
            endpoint: self.endpoint_host.clone(),
            token: token.into(),
        }
    }
}
```

- [x] **Step 4: Remove endpoint query parsing from the connection path**

```rust
impl ConnectedVoiceSession {
    pub(crate) async fn connect(voice: VoiceContext) -> Result<Self, AppError> {
        let handshake = VoiceHandshake::connect(voice.clone()).await?;
        Ok(Self::from_handshake(voice, handshake))
    }
}
```

- [x] **Step 5: Run the fake-peer handshake and existing runtime tests**

Run: `cargo test voice_handshake runtime_end_to_end_playback -v`

Expected: PASS with no dependency on synthetic `?udp=...&ssrc=...` endpoint data.

- [x] **Step 6: Commit**

```bash
git add tests/voice_handshake.rs tests/support/fake_discord.rs src/discord_voice/session.rs
git commit -m "test: remove synthetic fake voice endpoint contract"
```

## Task 2: Implement the real voice setup sequence

**Files:**
- Create: `src/discord_voice/protocol.rs`
- Create: `src/discord_voice/handshake.rs`
- Modify: `src/discord_voice/gateway.rs`
- Modify: `src/discord_voice/session.rs`
- Test: `tests/voice_gateway_v8.rs`
- Test: `tests/voice_handshake.rs`

- [x] **Step 1: Write the failing handshake-sequence test**

```rust
#[tokio::test]
async fn voice_handshake_performs_identify_discovery_select_protocol_and_session_description() {
    let fake = FakeDiscordPeer::spawn_real_shape().await;
    let voice = fake.voice_context("1", "2", "session-1", "token-1");

    let _session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(fake.saw_identify().await);
    assert!(fake.saw_select_protocol().await);
    assert!(fake.session_description_sent().await);
}
```

- [x] **Step 2: Run the failing test**

Run: `cargo test voice_handshake_performs_identify_discovery_select_protocol_and_session_description -v`

Expected: FAIL because the current gateway client does not implement the full setup flow.

- [x] **Step 3: Add protocol payload types and parsing helpers**

```rust
pub enum VoiceGatewayInbound {
    Hello { heartbeat_interval_ms: u64 },
    Ready { ssrc: u32, ip: String, port: u16, modes: Vec<String> },
    SessionDescription { mode: String, secret_key: Vec<u8>, dave_protocol_version: Option<u16> },
    Resumed,
    // ...
}

pub enum VoiceGatewayOutbound<'a> {
    Identify { server_id: &'a str, user_id: &'a str, session_id: &'a str, token: &'a str },
    SelectProtocol { address: &'a str, port: u16, mode: &'a str },
    Heartbeat { seq_ack: Option<u64>, nonce: u64 },
    Resume { server_id: &'a str, session_id: &'a str, token: &'a str, seq_ack: Option<u64> },
}
```

- [x] **Step 4: Implement a real handshake coordinator**

```rust
pub struct VoiceHandshakeResult {
    pub gateway: VoiceGatewayClient,
    pub udp_target: SocketAddr,
    pub ssrc: u32,
    pub session_description: SessionDescription,
}

pub struct VoiceHandshake;

impl VoiceHandshake {
    pub async fn connect(voice: VoiceContext) -> Result<VoiceHandshakeResult, AppError> {
        // connect websocket
        // send identify or resume
        // wait for Ready
        // perform UDP discovery
        // send Select Protocol
        // wait for Session Description
        // return assembled transport inputs
    }
}
```

- [x] **Step 4a: Use Context7 to confirm the `twilight` crate split before wiring controller-facing Discord types anywhere else**

Run a docs lookup for the exact current `twilight` crates needed for:

- gateway connection
- HTTP client
- model IDs and events

Expected: a short note in the working context naming the chosen crates so later tasks do not guess.

- [x] **Step 5: Run handshake and gateway tests**

Run: `cargo test voice_gateway_v8 voice_handshake -v`

Expected: PASS, including the existing `seq_ack` assertions and the new setup-flow assertions.

- [x] **Step 6: Commit**

```bash
git add src/discord_voice/protocol.rs src/discord_voice/handshake.rs src/discord_voice/gateway.rs src/discord_voice/session.rs tests/voice_gateway_v8.rs tests/voice_handshake.rs
git commit -m "feat: implement real voice setup flow"
```

## Task 3: Wire live transport packet protection

**Files:**
- Create: `src/discord_voice/protection.rs`
- Modify: `src/discord_voice/crypto.rs`
- Modify: `src/discord_voice/udp.rs`
- Modify: `src/discord_voice/session.rs`
- Test: `tests/voice_packet_protection.rs`

- [x] **Step 1: Write the failing protected-send test**

```rust
#[tokio::test]
async fn voice_udp_transport_applies_selected_packet_protection_before_send() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect_protected(
        fake.server_addr(),
        7,
        ProtectionContext::test_xchacha(),
    ).await.unwrap();

    transport.send_audio_frame(Bytes::from_static(b"opus-frame")).await.unwrap();

    let packet = fake.next_audio_packet().await;
    assert_ne!(&packet[12..], b"opus-frame");
}
```

- [x] **Step 2: Run the failing test**

Run: `cargo test voice_udp_transport_applies_selected_packet_protection_before_send -v`

Expected: FAIL because the current transport sends raw Opus directly after the RTP header.

- [x] **Step 3: Add a live packet-protection context**

```rust
pub struct ProtectionContext {
    mode: EncryptionMode,
    secret_key: Vec<u8>,
}

impl ProtectionContext {
    pub fn protect_packet(&self, rtp_header: &[u8], payload: &[u8]) -> Result<Vec<u8>, AppError> {
        // apply aead_*_rtpsize protection using the negotiated session key
    }
}
```

- [x] **Step 4: Apply protection in the live UDP send path**

```rust
pub async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), AppError> {
    let (sequence, timestamp) = self.sequence.advance();
    let rtp = self.packet_builder.build_header(sequence, timestamp);
    let packet = self.protection.protect_packet(&rtp, frame.as_ref())?;
    self.socket.send_to(&packet, self.server).await?;
    Ok(())
}
```

- [x] **Step 5: Run the transport tests**

Run: `cargo test voice_udp_transport voice_packet_protection -v`

Expected: PASS with protected packet payloads and existing mode-selection tests still green.

- [x] **Step 6: Commit**

```bash
git add src/discord_voice/protection.rs src/discord_voice/crypto.rs src/discord_voice/udp.rs src/discord_voice/session.rs tests/voice_packet_protection.rs
git commit -m "feat: protect live voice packets"
```

## Task 4: Wire DAVE into the live runtime path

**Files:**
- Modify: `src/discord_voice/dave.rs`
- Modify: `src/discord_voice/handshake.rs`
- Modify: `src/discord_voice/session.rs`
- Modify: `src/discord_voice/udp.rs`
- Test: `tests/dave_handshake.rs`
- Test: `tests/voice_handshake.rs`

- [x] **Step 1: Write the failing runtime-DAVE test**

```rust
#[tokio::test]
async fn voice_session_uses_dave_runtime_state_when_session_description_requires_it() {
    let fake = FakeDiscordPeer::spawn_with_dave().await;
    let voice = fake.voice_context("1", "2", "session-1", "token-1");

    let session = ConnectedVoiceSession::connect(voice).await.unwrap();

    assert!(session.dave_enabled());
    assert!(fake.saw_dave_transition().await);
}
```

- [x] **Step 2: Run the failing test**

Run: `cargo test voice_session_uses_dave_runtime_state_when_session_description_requires_it -v`

Expected: FAIL because the current DAVE code is not used by the live session path.

- [x] **Step 3: Expose runtime-usable DAVE state construction**

```rust
pub struct DaveRuntimeContext {
    pub protocol_version: u16,
    pub encryptor: DaveEncryptor,
    pub decryptor: DaveDecryptor,
}

impl DaveRuntimeContext {
    pub fn from_session_description(desc: &SessionDescription, ssrc: u32) -> Result<Self, AppError> {
        // construct ratchet-backed runtime encrypt/decrypt state
    }
}
```

- [x] **Step 4: Use DAVE in live readiness and packet send**

```rust
pub(crate) struct ConnectedVoiceSession {
    // ...
    dave: Option<DaveRuntimeContext>,
}

impl ConnectedVoiceSession {
    pub(crate) fn dave_enabled(&self) -> bool {
        self.dave.is_some()
    }
}
```

- [x] **Step 5: Run DAVE and handshake tests**

Run: `cargo test dave_handshake voice_handshake -v`

Expected: PASS with the original isolated DAVE tests still green and the new runtime DAVE-path test green.

- [x] **Step 6: Commit**

```bash
git add src/discord_voice/dave.rs src/discord_voice/handshake.rs src/discord_voice/session.rs src/discord_voice/udp.rs tests/dave_handshake.rs tests/voice_handshake.rs
git commit -m "feat: use dave in live voice runtime"
```

## Task 5: Add the real pacer and live stop semantics

**Files:**
- Modify: `src/playback/pacer.rs`
- Modify: `src/session/runtime.rs`
- Modify: `src/discord_voice/session.rs`
- Test: `tests/pacer_runtime.rs`
- Test: `tests/voice_udp_transport.rs`
- Test: `tests/runtime_end_to_end_playback.rs`

- [x] **Step 1: Write the failing pacing test**

```rust
#[tokio::test(start_paused = true)]
async fn runtime_emits_one_audio_frame_per_20ms_tick() {
    let mut pacer = AudioPacer::new();

    pacer.tick().await;
    pacer.tick().await;

    assert_eq!(pacer.emitted_frames(), 2);
}
```

- [x] **Step 2: Run the failing pacing test**

Run: `cargo test pacer_runtime -v`

Expected: FAIL because the current runtime drains the queue immediately and `pacer.rs` only contains constants.

- [x] **Step 3: Implement the pacer**

```rust
pub struct AudioPacer {
    ticker: tokio::time::Interval,
}

impl AudioPacer {
    pub fn new() -> Self {
        let mut ticker = tokio::time::interval(FRAME_DURATION);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self { ticker }
    }

    pub async fn wait_next(&mut self) {
        self.ticker.tick().await;
    }
}
```

- [x] **Step 4: Route live playback through the pacer and stop path**

```rust
while let Some(frame) = queue.pop() {
    pacer.wait_next().await;
    position_ms += frame.duration_ms;
    session.send_audio_frame(frame.data).await?;
}

session.stop_audio().await?;
```

- [x] **Step 4a: Emit explicit `Buffering` before `Playing` when startup prebuffering is active**

```rust
let buffering_event = {
    let mut state = self.state.write().await;
    state.state = SessionState::Buffering;
    SessionEventRecord::from_snapshot(SessionEventKind::Buffering, &state)
};
self.events.emit(buffering_event);
```

- [x] **Step 5: Run pacing and runtime tests**

Run: `cargo test pacer_runtime runtime_end_to_end_playback voice_udp_transport -v`

Expected: PASS with paced send semantics and five-silence-frame stop behavior still enforced.

- [x] **Step 6: Commit**

```bash
git add src/playback/pacer.rs src/session/runtime.rs src/discord_voice/session.rs tests/pacer_runtime.rs tests/voice_udp_transport.rs tests/runtime_end_to_end_playback.rs
git commit -m "feat: pace live playback and stop cleanly"
```

## Task 6: Complete recovery and voice-context refresh against the corrected transport

**Files:**
- Modify: `src/playback/recovery.rs`
- Modify: `src/session/runtime.rs`
- Modify: `src/discord_voice/session.rs`
- Modify: `src/discord_voice/handshake.rs`
- Test: `tests/recovery_reresolve.rs`
- Test: `tests/recovery_reopen.rs`
- Test: `tests/voice_context_rollover.rs`

- [x] **Step 1: Write the failing reconnect-position test**

```rust
#[tokio::test]
async fn update_voice_context_reconnects_and_resumes_after_last_emitted_position() {
    let harness = RolloverHarness::spawn().await;

    harness.play_until_position_ms(2_000).await;
    harness.update_voice_context().await;

    assert!(harness.resumed_after_position_ms(2_000).await);
}
```

- [x] **Step 2: Run the failing recovery tests**

Run: `cargo test recovery_reresolve recovery_reopen voice_context_rollover -v`

Expected: FAIL where reconnect or replay behavior does not align with the stricter spec.

- [x] **Step 3: Make recovery ordering explicit**

```rust
pub async fn recover(&mut self, video_id: &str, position_ms: u64) -> Result<PlaybackSource, AppError> {
    if let Ok(source) = self.try_reopen_existing(position_ms).await {
        return Ok(source);
    }

    let resolved = self.client.resolve_playback_source(video_id).await?;
    self.open_from_position(resolved, position_ms).await
}
```

- [x] **Step 4: Make voice-context refresh perform a real reconnect and resume**

```rust
async fn rollover_voice_context(&self, new_voice: VoiceContext) -> Result<(), AppError> {
    self.emit_voice_reconnecting().await;
    let replacement = ConnectedVoiceSession::connect(new_voice).await?;
    self.swap_voice_session(replacement).await;
    self.resume_current_track_from_last_emitted_position().await
}
```

- [x] **Step 5: Run recovery and rollover tests**

Run: `cargo test recovery_reresolve recovery_reopen voice_context_rollover -v`

Expected: PASS with resume occurring from the first packet whose end timestamp exceeds the last emitted position.

- [x] **Step 6: Commit**

```bash
git add src/playback/recovery.rs src/session/runtime.rs src/discord_voice/session.rs src/discord_voice/handshake.rs tests/recovery_reresolve.rs tests/recovery_reopen.rs tests/voice_context_rollover.rs
git commit -m "feat: resume correctly after recovery and reconnect"
```

## Task 7: Build the `twilight` staging controller

**Files:**
- Create: `src/bin/staging_live_check.rs`
- Create: `tests/live_contract_controller.rs`
- Modify: `Cargo.toml`
- Test: `tests/live_contract_controller.rs`

- [x] **Step 1: Write the failing controller-config test**

```rust
#[test]
fn staging_controller_requires_all_live_env_vars() {
    let err = StagingConfig::from_env_map(std::collections::HashMap::new()).unwrap_err();
    assert!(err.to_string().contains("BOT_TOKEN"));
}
```

- [x] **Step 2: Run the failing test**

Run: `cargo test live_contract_controller -v`

Expected: FAIL because the staging controller and its config type do not exist yet.

- [x] **Step 3: Add the controller binary and config loader**

```rust
struct StagingConfig {
    application_id: twilight_model::id::Id<twilight_model::id::marker::ApplicationMarker>,
    bot_token: String,
    test_guild_id: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    test_voice_channel_id: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    test_video_id: String,
    discord_voice_service_addr: String,
    ytmusic_addr: String,
}

impl StagingConfig {
    fn from_env() -> Result<Self, anyhow::Error> {
        // read APPLICATION_ID, BOT_TOKEN, TEST_GUILD_ID, TEST_VOICE_CHANNEL_ID,
        // TEST_VIDEO_ID, DISCORD_VOICE_SERVICE_ADDR, DISCORD_VOICE_SERVICE_YTMUSIC_ADDR
    }
}
```

- [x] **Step 3a: Pin the `twilight` crates explicitly in `Cargo.toml`**

```toml
[dependencies]
twilight-gateway = "..."
twilight-http = "..."
twilight-model = "..."
twilight-util = "..."
```

Expected: use the exact current crate set verified earlier, not guessed names or outdated split assumptions.

- [x] **Step 4: Implement the minimal `twilight` join-forward-play-assert flow**

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = StagingConfig::from_env()?;
    let ctx = TwilightVoiceJoinController::connect(&config).await?;
    let voice = ctx.join_and_wait_for_voice_context().await?;

    let mut client = DiscordVoiceControlClient::connect(config.discord_voice_service_addr.clone()).await?;
    client.join_voice(Request::new(JoinVoiceRequest { voice: Some(voice) })).await?;
    let mut events = client.subscribe_events(Request::new(SubscribeEventsRequest {})).await?.into_inner();
    client.play(Request::new(PlayRequest { video_id: config.test_video_id.clone() })).await?;

    assert_live_success(&mut events).await?;
    Ok(())
}
```

- [x] **Step 4a: Make the passing assertion logic match the spec exactly**

```rust
async fn assert_live_success(events: &mut Streaming<SessionEvent>) -> anyhow::Result<()> {
    let mut saw_voice_ready = false;
    let mut saw_playing = false;
    let mut saw_track_ended = false;
    let mut first_audio_at = None;

    while let Some(event) = events.message().await? {
        match SessionEventKind::try_from(event.kind)? {
            SessionEventKind::VoiceReady => saw_voice_ready = true,
            SessionEventKind::Playing => {
                saw_playing = true;
                first_audio_at.get_or_insert_with(std::time::Instant::now);
            }
            SessionEventKind::TrackEnded => {
                saw_track_ended = true;
                break;
            }
            SessionEventKind::FatalError | SessionEventKind::PlaybackInterrupted | SessionEventKind::VoiceReconnecting => {
                anyhow::bail!("unexpected live event: {:?}", event.kind);
            }
            _ => {}
        }
    }

    anyhow::ensure!(saw_voice_ready, "VoiceReady not observed");
    anyhow::ensure!(saw_playing, "Playing not observed");
    anyhow::ensure!(first_audio_at.is_some_and(|t| t.elapsed() >= std::time::Duration::from_secs(5)), "5 second live interval not satisfied");
    anyhow::ensure!(saw_track_ended, "TrackEnded not observed");
    Ok(())
}
```

- [x] **Step 5: Run controller unit tests**

Run: `cargo test live_contract_controller -v`

Expected: PASS for config parsing and local controller validation helpers.

- [x] **Step 6: Commit**

```bash
git add src/bin/staging_live_check.rs tests/live_contract_controller.rs Cargo.toml
git commit -m "feat: add twilight staging live controller"
```

## Task 8: Add the live staging workflow and operator docs

**Files:**
- Create: `.github/workflows/live-staging.yml`
- Modify: `README.md`
- Modify: `.gitignore` only if additional local staging artifacts need ignore rules
- Test: workflow syntax check via local inspection

- [x] **Step 1: Write the failing workflow contract note in docs**

```md
## Live staging validation

This repository requires a protected staging run using:

- `APPLICATION_ID`
- `BOT_TOKEN`
- `TEST_GUILD_ID`
- `TEST_VOICE_CHANNEL_ID`
- `TEST_VIDEO_ID`
```

- [x] **Step 2: Add the workflow**

```yaml
name: live-staging

on:
  workflow_dispatch:
  workflow_run:
    workflows: ["release-image"]
    types: [completed]

jobs:
  live-staging:
    if: github.event_name == 'workflow_dispatch' || github.event.workflow_run.conclusion == 'success'
    runs-on: [self-hosted, linux, discord-voice-staging]
    steps:
      - uses: actions/checkout@v4
      - run: podman run ... ytmusic-service --browser-config ./browser.json
      - run: cargo run --bin staging_live_check
        env:
          APPLICATION_ID: ${{ secrets.APPLICATION_ID }}
          BOT_TOKEN: ${{ secrets.BOT_TOKEN }}
          TEST_GUILD_ID: ${{ secrets.TEST_GUILD_ID }}
          TEST_VOICE_CHANNEL_ID: ${{ secrets.TEST_VOICE_CHANNEL_ID }}
          TEST_VIDEO_ID: ${{ secrets.TEST_VIDEO_ID }}
```

- [x] **Step 2a: Fail the workflow early if any required secret is missing**

```bash
test -n "$APPLICATION_ID"
test -n "$BOT_TOKEN"
test -n "$TEST_GUILD_ID"
test -n "$TEST_VOICE_CHANNEL_ID"
test -n "$TEST_VIDEO_ID"
```

- [x] **Step 3: Document the operator flow**

```md
The live staging run requires:

- a dedicated bot token
- a dedicated test guild
- a dedicated non-stage voice channel
- `ytmusic-service` started with `./browser.json`
- the staging controller binary `staging_live_check`
```

- [x] **Step 4: Validate docs and workflow formatting**

Run: `git diff --check && sed -n '1,220p' .github/workflows/live-staging.yml`

Expected: no diff-format errors and a readable workflow file with the required secrets.

- [x] **Step 5: Commit**

```bash
git add .github/workflows/live-staging.yml README.md .gitignore
git commit -m "ci: add live staging validation workflow"
```

## Task 9: Enforce the two release gates and run full verification

**Files:**
- Modify: existing CI workflow files as needed
- Modify: `README.md` if release-gate wording needs tightening
- Test: full repo verification
- Modify: `AGENTS.md` only if execution notes need a final refresh

- [x] **Step 1: Tighten the release-gate workflow rules**

```yaml
jobs:
  fake-peer-ci:
    # normal branch and PR gate

  live-staging:
    # protected manual or release gate using real secrets

  release-ready:
    needs: [fake-peer-ci, live-staging]
```

- [x] **Step 2: Run the full local verification set**

Run:

```bash
cargo test -v
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: all commands succeed locally before requesting any higher-level review.

- [x] **Step 3: Run the fake-peer critical subset explicitly**

Run:

```bash
cargo test voice_handshake voice_gateway_v8 voice_udp_transport pacer_runtime runtime_end_to_end_playback recovery_reresolve recovery_reopen voice_context_rollover -v
```

Expected: PASS for the corrected protocol and runtime behavior.

- [x] **Step 4: Record the required live staging command**

Run:

```bash
cargo run --bin staging_live_check
```

Expected: in a fully prepared staging environment, the controller exits `0` only after real join, forward, play, sustained media interval, and cleanup succeed.

- [x] **Step 4a: Record the mandatory live staging success evidence in the final implementation notes**

Include:

- commit SHA tested
- runner type used
- whether `ytmusic-service` was started with `./browser.json`
- whether authentic voice context was acquired
- whether `VoiceReady`, `Playing`, and `TrackEnded` were observed
- whether the 5-second live interval passed
- whether cleanup succeeded

- [x] **Step 5: Commit**

```bash
git add .github/workflows README.md
git commit -m "build: enforce production release gates"
```

## Spec coverage check

- Real forwarded voice-context handling: Task 1, Task 2, Task 6, Task 7
- Real Discord voice handshake: Task 2
- UDP discovery and setup: Task 2
- Transport encryption in live path: Task 3
- DAVE/E2EE in live runtime path: Task 4
- Stable 20 ms pacing: Task 5
- Speaking and five-silence stop behavior in live path: Task 5
- Recovery and reconnect semantics: Task 6
- `twilight` staging controller: Task 7
- `ytmusic-service` live test using `./browser.json`: Task 8
- Both release gates required: Task 8, Task 9

## Self-review

- Placeholder scan: no `TODO`, `TBD`, or “implement later” placeholders remain.
- Type consistency: all new plan types use consistent names across tasks:
  - `VoiceHandshake`
  - `VoiceHandshakeResult`
  - `ProtectionContext`
  - `DaveRuntimeContext`
  - `AudioPacer`
  - `StagingConfig`
- Scope check: this is one corrective implementation stream with dependent tasks, not multiple independent specs.
- Fresh-session check: the plan now includes the current project snapshot, exact staging env contract, exact expected live event sequence, runner policy, and the `docs/superpowers` force-add note.
