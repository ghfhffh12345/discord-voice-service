# DAVE Late-Listener Epoch Fix Design

## Summary

Fix the remaining established-group DAVE failure so `scripts/ci/run_local_live_staging.sh` can prove real audio delivery.

The current blocker is no longer helper orchestration. The helper reaches `VoiceReady`, but the first late-listener DAVE transition after join is rejected during `Play` with:

- service error: `invalid state: voice dave commit invalid`
- underlying OpenMLS error: `Wrong Epoch: message.epoch() 0 != 1 self.group_context().epoch()`

The fix should make the post-join active DAVE runtime and the first late-listener transition agree on the same established-group MLS epoch. Commit validation must remain strict.

## Goals

- Make the established-group late-listener path valid enough for `scripts/ci/run_local_live_staging.sh` to pass.
- Keep DAVE fail-closed behavior for truly invalid post-join transitions.
- Cover the fix with a focused regression that exercises the late-listener path before more audio is sent.

## Non-Goals

- Do not add a broad DAVE resync or recovery mechanism.
- Do not weaken commit validation to accept stale or mismatched epochs.
- Do not redesign unrelated CI or local helper behavior that is already working.
- Do not broaden this into a full active-session DAVE hardening project.

## Problem Statement

For an already-established DAVE group, the service should:

1. finish join
2. store one canonical active DAVE runtime
3. receive the first late-listener transition for that same group state
4. apply the transition before continuing media send

The current path diverges at step 3. After `VoiceReady`, the service receives a late-listener commit that behaves like it was built from an older MLS epoch than the runtime the service already holds. OpenMLS correctly rejects that commit, and playback fails before valid audio is proven.

## Architecture Boundary

The minimal fix should treat this as a boundary problem between initial established-group join and active-session transitions.

- `crates/discord-voice-service-voice/src/handshake.rs` is responsible for producing one canonical active DAVE runtime after join.
- `crates/discord-voice-service-voice/src/session.rs` is responsible for accepting only post-join transitions that are valid relative to that runtime.
- `crates/discord-voice-service-test-support/src/fake_discord.rs` and the focused regressions are responsible for modeling the same established-group epoch and transition shape that the real proof path depends on.

The fix should not make `session.rs` more permissive. It should make the producer and consumer agree on the same established-group state.

## Proposed Design

### 1. Canonical established-group handoff

The join path must leave `ConnectedVoiceSession` with an active runtime that fully reflects the established group state used by the next gateway-driven listener transition.

For this issue, "joined" cannot mean only "ready enough to emit `VoiceReady`." It must mean the stored runtime is semantically at the same epoch and roster base that the first post-join late-listener transition expects.

Any narrow handoff adjustment needed to preserve that canonical state belongs in the handshake-to-session boundary, not in later media-send recovery code.

### 2. Strict active-session transition handling

`session.rs` should keep the current strict model:

- drain pending DAVE events before media send
- stage only transitions that apply to the current runtime
- reject stale or mismatched commit transitions

The existing `voice dave commit invalid` failure is a useful signal and should remain the error for invalid post-join commits. Success means the real established-group path stops producing that invalid commit, not that the error is hidden.

### 3. Protocol-consistent established-group fixture

The established-group fake/live producer path must generate the first late-listener transition from the same creator/group state that the connected runtime is supposed to share.

If the fixture currently emits a transition from a stale creator epoch, it is modeling the wrong proof target. The fix may therefore touch `fake_discord.rs` alongside service/runtime code, but only to keep the proof fixture aligned with the real established-group state transition the service is expected to handle.

## Data Flow

The corrected minimal path is:

1. established-group join completes
2. a canonical active DAVE runtime is stored
3. the first late-listener proposal and commit are produced from the matching established creator/group epoch
4. `ConnectedVoiceSession` drains and applies that transition before sending more media
5. media send continues with the updated DAVE group state
6. local live staging reaches `Playing`, completes naturally, and proves observer-side audio

## Error Handling

Error handling stays fail-closed.

- If the post-join late-listener transition still does not match the active runtime, `session.rs` should reject it.
- The helper should continue surfacing the service log and structured evidence artifact.
- No retry, rejoin, or silent fallback behavior is part of this fix.

This keeps the proof meaningful: if local live staging passes, it passes because the DAVE transition path is valid, not because invalid transitions were tolerated.

## Testing Strategy

### Focused regression

Keep and pass the targeted established-group late-listener regression in `crates/discord-voice-service-voice/tests/voice_send_dave.rs`. That test should prove:

- join an established group
- observe a usable connected DAVE runtime
- inject the first late-listener transition
- send more audio successfully without loosening commit validation

### Supporting runtime regression

Keep the runtime-level late-transition playback regression green so the service still drains pending DAVE work before meaningful media send.

### Negative coverage

Keep existing invalid-commit behavior covered so obviously bad transitions still fail closed.

### Real proof gate

`scripts/ci/run_local_live_staging.sh` must pass and leave success evidence showing:

- `saw_voice_ready=true`
- `saw_playing=true`
- `saw_track_ended=true`
- observer audio thresholds satisfied with non-silent decoded audio

## Acceptance Criteria

The work is done when all of the following are true:

- the focused established-group late-listener regression is green
- the supporting runtime late-transition regression remains green
- invalid post-join commit cases still fail closed
- `scripts/ci/run_local_live_staging.sh` passes against real local secrets and Discord connectivity

## Risks

- The remaining bug may sit partly in the fixture model and partly in the handshake/session handoff; the fix should follow the epoch evidence rather than assume only one side is wrong.
- The real Discord path may contain ordering details that the fake path still does not represent; if so, the fixture should be updated only enough to match the proven live behavior for this path.
- Because `docs/superpowers/` is ignored in this repo, the design-doc commit for workflow compliance must force-add only this spec file.
