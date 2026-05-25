use std::fs;

const OCCUPIED_LISTENER_CONTRACT: &str =
    "During live staging, human listeners may remain in the channel while the staging bot validates playback against the short dedicated validation track.";
const NATURAL_END_SUCCESS_CONTRACT: &str =
    "Live-staging success waits for the natural end of the validation track before the run is treated as release-ready.";

#[test]
fn readme_publishes_the_hosted_live_staging_contract() {
    let readme = fs::read_to_string("../../README.md").expect("README should exist");

    assert!(readme.contains("DISCORD_VOICE_SERVICE_BIND_ADDR"));
    assert!(readme.contains("DISCORD_VOICE_SERVICE_URI"));
    assert!(readme.contains("exact container artifact"));
    assert!(readme.contains("GitHub-hosted"));
    assert!(readme.contains("BROWSER_JSON"));
    assert!(readme.contains("protected `live-staging` environment"));
    assert!(readme.contains("candidate manifest digest"));
    assert!(readme.contains("rollback"));
    assert!(readme.contains("short dedicated validation track"));
    assert!(readme.contains(OCCUPIED_LISTENER_CONTRACT));
    assert!(readme.contains(NATURAL_END_SUCCESS_CONTRACT));
    assert!(!readme.contains("DISCORD_VOICE_SERVICE_ADDR"));
    assert!(!readme.contains("self-hosted runner profile"));
    assert!(!readme.contains("5-second live interval"));
    assert!(!readme.contains("STAGING_BROWSER_JSON_SOURCE_PATH"));
}
