use std::fs;

const LOCAL_LIVE_STAGING_CONTRACT: &str =
    "For local real-Discord live staging, load secrets from `.env`, load `BROWSER_JSON` from `./browser.json`, start a source-built `discord-voice-service`, and then run `scripts/ci/run_local_live_staging.sh`.";
const SEND_SIDE_SUCCESS_CONTRACT: &str =
    "Live-staging success is based on Discord-supported send-side proof: authentic voice context, VoiceReady, Playing, natural TrackEnded, and no reconnect/interruption/fatal error during validation.";

#[test]
fn readme_publishes_the_hosted_live_staging_contract() {
    let readme = fs::read_to_string("../../README.md").expect("README should exist");

    assert!(readme.contains("DISCORD_VOICE_SERVICE_BIND_ADDR"));
    assert!(readme.contains("DISCORD_VOICE_SERVICE_URI"));
    assert!(readme.contains("exact container artifact"));
    assert!(readme.contains("GitHub-hosted"));
    assert!(readme.contains("BROWSER_JSON"));
    assert!(readme.contains(LOCAL_LIVE_STAGING_CONTRACT));
    assert!(readme.contains(SEND_SIDE_SUCCESS_CONTRACT));
    assert!(readme.contains("protected `live-staging` environment"));
    assert!(readme.contains("candidate manifest digest"));
    assert!(readme.contains("rollback"));
    assert!(readme.contains("short dedicated validation track"));
    assert!(!readme.contains("DISCORD_VOICE_SERVICE_ADDR"));
    assert!(!readme.contains("self-hosted runner profile"));
    assert!(!readme.contains("5-second live interval"));
    assert!(!readme.contains("STAGING_BROWSER_JSON_SOURCE_PATH"));
    assert!(!readme.contains("OBSERVER_APPLICATION_ID"));
    assert!(!readme.contains("OBSERVER_BOT_TOKEN"));
    assert!(!readme.contains("observer bot"));
}
