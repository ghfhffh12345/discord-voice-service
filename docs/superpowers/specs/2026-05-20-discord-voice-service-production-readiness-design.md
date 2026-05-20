# discord-voice-service production readiness design

Date: 2026-05-20

## Purpose

This document defines the remaining production-readiness work for `discord-voice-service`.

The repository already has a working real-Discord path for controlled staging validation. The remaining gaps are not about broadening feature scope. They are about making the existing narrow service honestly releasable, operationally supportable, and explicit about what "production-ready" means.

This design keeps the intended service scope narrow:

- single guild only
- single active session per process
- bot-assisted voice-context boundary
- Opus-in-WebM passthrough only
- no transcoding
- no multi-guild scheduler
- self-hosted live validation as a supported long-term constraint

## Production-readiness definition

For this repository, `production-ready` means all of the following are true:

- the service is supported for a narrow single-guild, single-session deployment model
- the public runtime contract is explicit, internally consistent, and enforced by startup validation
- the exact container artifact that is released has passed both protocol-level CI and real Discord live staging
- the supported self-hosted runner contract is documented, preflight-checked, and treated as part of the operating model
- supported failure modes are bounded, observable, and deterministic rather than undefined

This design does not redefine the product into a broader service. It makes the current product safe to describe and operate as production-ready within its intended scope.

## Non-goals

This design does not add:

- multi-guild concurrency
- multi-session failover
- broad high-availability guarantees
- audio decode or transcode fallback
- AAC or MP4 playback support
- general GitHub-hosted live-validation parity
- backward-compatibility shims for temporary pre-production configuration names

## Release architecture

The release path must change from:

`validate rebuilt source -> publish image`

to:

`build candidate image -> validate exact candidate image -> promote exact validated image`

The candidate release flow is:

1. build a candidate container image from the release commit
2. assign it a candidate tag, digest-only reference, or other non-promoted artifact identity
3. require fake-peer CI to have passed for the same commit
4. run live staging against that exact candidate image on the supported self-hosted runner class
5. only if both gates pass, promote that same image digest to the public release tags
6. record release evidence against that digest

This gives the repository a defensible production claim: the artifact that passed live Discord validation is the artifact that was released.

### Gate meanings

The two required gates have different responsibilities:

- `fake-peer CI`
  - proves code-path correctness, regression resistance, and protocol-shape coverage for the candidate commit
- `live staging`
  - proves the exact released artifact works in the supported real environment

Neither gate substitutes for the other. Production readiness requires both.

## Public configuration contract

The overloaded `DISCORD_VOICE_SERVICE_ADDR` contract must be removed completely.

The canonical configuration surface becomes:

- `DISCORD_VOICE_SERVICE_BIND_ADDR`
  - bare socket bind address for the service process
  - example: `127.0.0.1:55051`
- `DISCORD_VOICE_SERVICE_URI`
  - gRPC base URI used by controllers, probes, and external clients
  - example: `http://127.0.0.1:55051`
- `DISCORD_VOICE_SERVICE_YTMUSIC_ADDR`
  - gRPC base URI for `ytmusic-service`
  - example: `http://127.0.0.1:50051`

This split is the only supported contract. There is no compatibility layer, alias, or grace-period support for the old overloaded variable name.

### Contract rules

- the service process reads `DISCORD_VOICE_SERVICE_BIND_ADDR`
- staging tooling and client-side helpers read `DISCORD_VOICE_SERVICE_URI`
- documentation, examples, tests, workflows, and operator commands must all use the same names and meanings
- startup validation must fail fast on missing or invalid values

### Required validation behavior

The service or controller startup path must emit clear operator-usable failures for:

- invalid bind-address parsing
- invalid gRPC URI parsing
- missing required values
- unsupported or contradictory deployment assumptions when detectable at startup
- missing staging-only variables in the live-controller path

## Supported production envelope

The repository must state the supported operating envelope directly.

Supported:

- one process handling one active voice session
- one deployment serving one guild at a time
- playback input expressed as `videoId`
- `ytmusic-service` used as the real source of metadata and deciphered URLs
- WebM/Opus passthrough only
- self-hosted live validation and release proof on the supported runner class
- deployments that allow outbound internet plus inbound UDP replies suitable for Discord voice

Unsupported:

- multi-guild coordination
- concurrent independent voice sessions
- transcoding or alternative media pipelines
- high-availability or seamless failover guarantees
- broad claims that GitHub-hosted runners provide equivalent live-validation support
- ambiguous partial support for configurations outside the documented runner/network model

The goal is to present this service as intentionally narrow, not half-finished. Unsupported cases must be rejected or documented as unsupported rather than left open to interpretation.

## Validation workflow hardening

The live workflow must become a reproducible validation harness for a supported runner profile, not just a successful CI script.

### Supported runner profile

The design assumes a named supported self-hosted runner class for live validation. That profile includes:

- Node 24-compatible GitHub Actions support
- Podman installed and usable
- any local toolchain still required by controller-side tooling
- runner-local `browser.json` material available through a documented path contract
- outbound internet access
- inbound UDP replies suitable for Discord voice transport
- protected environment secrets and approvals configured for staging

### Preflight behavior

The workflow must fail in a dedicated prerequisites phase before expensive work begins.

That preflight must validate at least:

- required tools exist and are runnable
- required secrets and variables are present
- `browser.json` source material is available at the expected path
- the workflow is about to validate the expected candidate artifact identity
- the runner metadata needed for evidence collection is available

Preflight failures must produce precise diagnostics and a short operator checklist in the job summary.

### Exact-artifact live validation

The live gate must run against the exact candidate image digest selected for release promotion.

It must not:

- rebuild the service binary from the checked-out source as the thing being validated
- validate one artifact and publish a different one
- hide the artifact identity behind mutable tags with no recorded digest

### Evidence requirements

Every live run must emit structured evidence containing at least:

- run timestamp
- git commit SHA
- candidate image digest
- runner class or labels
- `ytmusic-service` image ref
- browser-config source mode
- whether authentic voice context was acquired
- whether `VoiceReady` was observed
- whether `Playing` was observed
- whether `TrackEnded` was observed
- whether the minimum live interval passed
- whether cleanup and leave succeeded
- explicit failure reason when the run fails

The evidence must be easy to audit in workflow logs and summaries. It does not require a separate database.

### Validation lanes

The design defines two live-validation lanes:

- `release gate lane`
  - one exact-artifact live pass required before promotion
- `confidence lane`
  - repeated live runs on the supported self-hosted runner class to detect drift, runner regressions, Discord-side instability, and operational flakiness

The confidence lane does not need to block every release immediately, but it is required for ongoing production confidence.

## Runtime recovery hardening scope

This design separates required production hardening from broader resilience work.

For this service, production readiness does not require invisibility under all failures. It requires that common supported failure classes have defined and observable outcomes.

### Required production recovery scope

The production bar covers:

- interrupted upstream media fetch with reopen or re-resolve behavior
- forwarded voice-context churn that requires controlled reconnect behavior
- Discord-side resume failure falling back to reconnect using the latest valid voice context
- service restart from a clean process state
- deterministic failure when recovery cannot safely continue

### Explicitly not promised

The service does not need to promise:

- seamless continuity across process restarts
- cross-node recovery
- multi-session failover
- transparent recovery under every Discord-side disruption

### Required recovery outcomes

Every supported recovery path must end in one of three operator-visible outcomes:

- `recovered and continued`
- `reconnected and resumed with bounded position drift`
- `failed deterministically and requires a new command`

That is sufficient for a narrow production service. Undefined recovery behavior is not.

## Repeated live-confidence scenarios

The confidence lane should cover repeated real-run scenarios such as:

- voice reconnect after refreshed forwarded context
- service restart followed by clean rejoin and replay behavior
- transient upstream media interruption with recovery attempt
- Discord-side resume failure leading to full reconnect
- cleanup and leave behavior after partial failures

These scenarios do not all need to be release-blocking immediately. They do need to become an explicit confidence matrix for the supported runner class.

## Operational workflow requirements

The workflow's current sensitive assumptions must be formalized rather than left as tribal knowledge.

Operational requirements include:

- documented runner preparation steps
- documented handling for runner-local `browser.json`
- explicit protected-environment ownership and approval expectations
- clearly stated network requirements for real UDP media validation
- predictable cleanup behavior for containers, temp material, and service state after success or failure
- runbook-level guidance for diagnosing preflight, live-connect, media, and cleanup failures

The workflow should be able to explain why a run failed in terms an operator can act on.

## Production acceptance criteria

Production acceptance must be defined across five areas.

### 1. Artifact correctness

- the promoted GHCR image digest is the exact artifact that passed live staging
- the release record includes commit SHA, image digest, gate results, and runner class

### 2. Configuration correctness

- the public contract uses distinct variables for bind address and service URI
- startup and controller validation fail fast on invalid or missing config
- README, workflow docs, examples, and automation all use the same contract

### 3. Operational correctness

- the supported self-hosted runner profile is documented and preflight-checked
- required materials such as `browser.json`, secrets, Podman, and network assumptions are explicit and auditable
- the live workflow emits structured run evidence and actionable failure diagnostics

### 4. Runtime correctness

- the service can establish voice, play, stop, and leave within the supported single-session envelope
- documented recovery paths either recover within bounded behavior or fail deterministically with observability
- unsupported cases are rejected clearly instead of being treated as ambiguous partial support

### 5. Support-scope correctness

- the repository states plainly that this is production-ready for controlled single-guild, single-session use
- the documentation explicitly excludes broader guarantees such as multi-guild concurrency, high-availability failover, transcoding, and broad deployment portability

## Recommended milestone shape

The remaining production-readiness work should be treated as one focused milestone with four deliverables:

1. `Release promotion redesign`
   Build the candidate image first, validate the exact digest, and promote only after gates pass.

2. `Contract cleanup`
   Replace the overloaded address variable with distinct bind and URI variables everywhere, with no compatibility path.

3. `Operationalization`
   Document and preflight-check the supported self-hosted runner contract, structured evidence output, and operator runbook expectations.

4. `Confidence matrix`
   Define and run recurring live scenarios for reconnect, restart, and transient-failure coverage on the supported runner class.

## Completion statement

This production-readiness milestone is complete when `discord-voice-service` can honestly claim all of the following at once:

- it is production-ready for its intentionally narrow single-guild, single-session scope
- its public configuration and operating contract are explicit and internally consistent
- the exact released artifact has passed both fake-peer CI and real Discord live validation
- its self-hosted validation model is documented as a supported constraint rather than hidden as workflow fragility
- its common supported failure modes are bounded, observable, and operationally understandable
