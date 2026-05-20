use std::fs;

#[test]
fn readme_publishes_the_new_production_contract() {
    let readme = fs::read_to_string("README.md").expect("README should exist");

    assert!(readme.contains("DISCORD_VOICE_SERVICE_BIND_ADDR"));
    assert!(readme.contains("DISCORD_VOICE_SERVICE_URI"));
    assert!(readme.contains("production-ready for controlled single-guild, single-session use"));
    assert!(readme.contains("exact container artifact"));
    assert!(readme.contains("self-hosted runner profile"));
    assert!(readme.contains("protected `live-staging` environment"));
    assert!(readme.contains("candidate digest"));
    assert!(readme.contains("rollback"));
    assert!(!readme.contains("DISCORD_VOICE_SERVICE_ADDR"));
}
