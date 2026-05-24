use std::fs;

#[test]
fn live_staging_workflow_uses_github_hosted_runner_and_secret_browser_json() {
    let workflow = fs::read_to_string("../../.github/workflows/live-staging.yml")
        .expect("live-staging workflow should exist");
    let preflight = fs::read_to_string("../../scripts/ci/live_staging_preflight.sh")
        .expect("live staging preflight script should exist");
    let run_script = fs::read_to_string("../../scripts/ci/run_live_staging.sh")
        .expect("live staging runner script should exist");

    assert!(workflow.contains("runs-on: ubuntu-24.04"));
    assert!(workflow.contains("environment: live-staging"));
    assert!(workflow.contains("BROWSER_JSON: ${{ secrets.BROWSER_JSON }}"));
    assert!(workflow.contains("scripts/ci/live_staging_preflight.sh"));
    assert!(workflow.contains("scripts/ci/run_live_staging.sh"));
    assert!(workflow.contains("DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF"));
    assert!(workflow.contains("${DISCORD_VOICE_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(workflow.contains("${YTMUSIC_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(workflow.contains("packages: read"));
    assert!(workflow.contains("docker/login-action@v3"));
    assert!(!workflow.contains("self-hosted"));
    assert!(!workflow.contains("discord-voice-staging"));
    assert!(!workflow.contains("STAGING_BROWSER_JSON_SOURCE_PATH"));

    assert!(preflight.contains("BROWSER_JSON"));
    assert!(!preflight.contains("STAGING_BROWSER_JSON_SOURCE_PATH"));

    assert!(run_script.contains("printf '%s' \"${BROWSER_JSON}\""));
    assert!(run_script.contains("docker pull \"${service_image_ref}\""));
    assert!(run_script.contains("docker run -d"));
    assert!(!run_script.contains("podman "));
}

#[test]
fn fake_peer_ci_runs_a_single_mock_gate() {
    let workflow = fs::read_to_string("../../.github/workflows/fake-peer-ci.yml")
        .expect("fake-peer workflow should exist");

    assert!(workflow.contains("cargo fmt --all --check"));
    assert!(workflow.contains("cargo clippy --workspace --all-targets --all-features -- -D warnings"));
    assert_eq!(workflow.matches("cargo test --workspace -v").count(), 1);
    assert!(!workflow.contains("Run fake-peer critical subset"));
}

#[test]
fn release_workflow_promotes_validated_candidate_digest() {
    let workflow = fs::read_to_string("../../.github/workflows/release-image.yml")
        .expect("release-image workflow should exist");
    let promote_script = fs::read_to_string("../../scripts/ci/promote_candidate_image.sh")
        .expect("promote candidate image script should exist");

    assert!(workflow.contains("candidate-"));
    assert!(workflow.contains("needs:\n      - verify-fake-peer-ci\n      - build-candidate"));
    assert!(workflow.contains(
        "discord_voice_service_image_ref: ${{ needs.build-candidate.outputs.image_repo }}@${{ needs.build-candidate.outputs.candidate_digest }}"
    ));
    assert!(workflow.contains(
        "discord_voice_service_image_digest: ${{ needs.build-candidate.outputs.candidate_digest }}"
    ));
    assert!(workflow.contains("scripts/ci/promote_candidate_image.sh"));
    assert!(promote_script.contains("skopeo copy --all"));
    assert!(promote_script.contains("\"docker://${SOURCE_IMAGE_REPO}@${SOURCE_IMAGE_DIGEST}\""));
    assert!(promote_script.contains("\"docker://${SOURCE_IMAGE_REPO}:${tag}\""));
}

#[test]
fn live_confidence_workflow_exists() {
    let workflow = fs::read_to_string("../../.github/workflows/live-confidence.yml")
        .expect("live-confidence workflow should exist");

    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("workflow_dispatch:"));
    assert!(workflow.contains("uses: ./.github/workflows/live-staging.yml"));
}
