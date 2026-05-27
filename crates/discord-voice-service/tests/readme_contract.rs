use std::fs;

const LOCAL_LIVE_STAGING_CONTRACT: &str = "For local real-Discord live staging, load secrets from `.env`, load `BROWSER_JSON` from `./browser.json`, start a source-built `discord-voice-service`, and then run `scripts/ci/run_local_live_staging.sh`.";
const OBSERVER_SECRET_CONTRACT: &str = "Protected live staging requires `OBSERVER_BOT_TOKEN` for the muted, non-deafened observer identity that validates receive-side audio.";
const RECEIVE_SIDE_SUCCESS_CONTRACT: &str = "Live-staging success requires observer receive-side proof: authentic voice context, VoiceReady, Playing, natural TrackEnded, at least 120 observed packets, at least 3000 ms decoded audio, at least 1000 ms non-silent audio, and no reconnect/interruption/fatal error during validation.";
const EVIDENCE_ARTIFACT_CONTRACT: &str = "Live-staging always uploads a structured observer evidence artifact summarizing observed packets, decoded audio, non-silent audio, and failure_reason.";

#[test]
fn readme_publishes_the_hosted_live_staging_contract() {
    let readme = fs::read_to_string("../../README.md").expect("README should exist");

    assert!(readme.contains("DISCORD_VOICE_SERVICE_BIND_ADDR"));
    assert!(readme.contains("DISCORD_VOICE_SERVICE_URI"));
    assert!(readme.contains("exact container artifact"));
    assert!(readme.contains("GitHub-hosted"));
    assert!(readme.contains("BROWSER_JSON"));
    assert!(readme.contains("OBSERVER_BOT_TOKEN"));
    assert!(readme.contains(LOCAL_LIVE_STAGING_CONTRACT));
    assert!(readme.contains(OBSERVER_SECRET_CONTRACT));
    assert!(readme.contains(RECEIVE_SIDE_SUCCESS_CONTRACT));
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
