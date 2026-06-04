use std::fs;

const LOCAL_LIVE_STAGING_CONTRACT: &str = "For local real-Discord live staging, run `scripts/ci/run_local_live_staging.sh`; the helper loads secrets from `.env`, loads `BROWSER_JSON` from `./browser.json`, starts a disposable local `ytmusic-service` container and CPU-contention container, waits for `ytmusic-service` gRPC readiness, then starts a source-built `discord-voice-service` with the HTTP read stress profile before running observer validation.";
const LOCAL_LIVE_STAGING_IMAGE_OVERRIDE_CONTRACT: &str = "For reproducible local live staging, optionally set `YTMUSIC_SERVICE_IMAGE_REF`; otherwise the helper defaults to `ghcr.io/ghfhffh12345/ytmusic-service:latest`.";
const OBSERVER_SECRET_CONTRACT: &str = "Protected live staging requires `OBSERVER_BOT_TOKEN` for the muted, non-deafened observer identity that validates receive-side audio.";
const RECEIVE_SIDE_SUCCESS_CONTRACT: &str = "Live-staging success requires observer receive-side proof: authentic voice context, VoiceReady, Playing, pause without leaving the voice channel, no service audio or speaking state during the paused interval, resume without voice-channel rejoin, natural TrackEnded, at least 120 observed packets, at least 3000 ms decoded audio, at least 1000 ms non-silent audio, and no reconnect/interruption/fatal error during validation.";
const PLAYBACK_METRICS_SUCCESS_CONTRACT: &str = "Live-staging success requires service-side playback stability metrics from `GetPlaybackMetrics`, including RTP interval stats, sender lateness, buffer depth, refill durations, underruns, inserted silence, and interruption counters.";
const CONSTRAINED_PROFILE_SUCCESS_CONTRACT: &str = "Live-staging success runs a constrained profile with CPU contention, a service CPU limit, and slow/jittery HTTP media reads configured by the `LIVE_STAGING_*` variables.";
const LONG_TRACK_SUCCESS_CONTRACT: &str = "Live-staging success requires a distinct long-track staging probe using `TEST_LONG_VIDEO_ID`; the probe must reach at least `LIVE_STAGING_LONG_TRACK_MIN_PACKETS` RTP packets before Stop and must satisfy the same RTP interval, sender lateness, and underrun budgets.";
const ACTIVE_INTERRUPT_SUCCESS_CONTRACT: &str = "After natural playback metrics are captured, live-staging success also starts fresh probe playbacks and validates active `UpdateVoiceContext` reconnect rollover, `Stop`, and `LeaveVoice` while those probes are actively Playing.";
const EVIDENCE_ARTIFACT_CONTRACT: &str = "Live-staging always uploads a structured evidence artifact summarizing the constrained profile, slow/jittery HTTP read settings, ignored invalid Resume, ignored redundant Pause, pause silence, resume packets, active reconnect rollover, active Stop, active LeaveVoice, observed packets, decoded audio, non-silent audio, natural playback stability metrics, reconnect probe metrics, long-track metrics, and failure_reason.";

#[test]
fn readme_publishes_the_hosted_live_staging_contract() {
    let readme = fs::read_to_string("../../README.md").expect("README should exist");

    assert!(readme.contains("DISCORD_VOICE_SERVICE_BIND_ADDR"));
    assert!(readme.contains("DISCORD_VOICE_SERVICE_URI"));
    assert!(readme.contains("exact container artifact"));
    assert!(readme.contains("GitHub-hosted"));
    assert!(readme.contains("BROWSER_JSON"));
    assert!(readme.contains("OBSERVER_BOT_TOKEN"));
    assert!(readme.contains("TEST_LONG_VIDEO_ID"));
    assert!(readme.contains("LIVE_STAGING_HTTP_READ_DELAY_MS"));
    assert!(readme.contains("LIVE_STAGING_HTTP_READ_JITTER_MS"));
    assert!(readme.contains(LOCAL_LIVE_STAGING_CONTRACT));
    assert!(readme.contains(LOCAL_LIVE_STAGING_IMAGE_OVERRIDE_CONTRACT));
    assert!(readme.contains(OBSERVER_SECRET_CONTRACT));
    assert!(readme.contains(RECEIVE_SIDE_SUCCESS_CONTRACT));
    assert!(readme.contains(PLAYBACK_METRICS_SUCCESS_CONTRACT));
    assert!(readme.contains(CONSTRAINED_PROFILE_SUCCESS_CONTRACT));
    assert!(readme.contains(LONG_TRACK_SUCCESS_CONTRACT));
    assert!(readme.contains(ACTIVE_INTERRUPT_SUCCESS_CONTRACT));
    assert!(readme.contains(EVIDENCE_ARTIFACT_CONTRACT));
    assert!(readme.contains("scripts/ci/run_local_live_staging.sh"));
    assert!(readme.contains("protected `live-staging` environment"));
    assert!(readme.contains("candidate manifest digest"));
    assert!(readme.contains("rollback"));
    assert!(readme.contains("short dedicated validation track"));
    assert!(!readme.contains("For a manual or local staging run"));
    assert!(!readme.contains("DISCORD_VOICE_SERVICE_ADDR"));
    assert!(!readme.contains("self-hosted runner profile"));
    assert!(!readme.contains("5-second live interval"));
    assert!(!readme.contains("STAGING_BROWSER_JSON_SOURCE_PATH"));
    assert!(!readme.contains("OBSERVER_APPLICATION_ID"));
}
