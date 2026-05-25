use std::fs;

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
    assert!(readme.contains("human listeners may remain in the channel"));
    assert!(readme.contains("natural end of the validation track"));
    assert!(readme.contains("success waits for the natural end of the validation track"));
    assert!(!readme.contains("DISCORD_VOICE_SERVICE_ADDR"));
    assert!(!readme.contains("self-hosted runner profile"));
    assert!(!readme.contains("5-second live interval"));
    assert!(!readme.contains("STAGING_BROWSER_JSON_SOURCE_PATH"));
}
