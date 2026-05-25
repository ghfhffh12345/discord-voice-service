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
    assert!(run_script.contains("docker run --rm --network \"${network_name}\""));
    assert!(run_script.contains("ytmusic_ready_check"));
    assert!(run_script.contains("-v \"${ytmusic_probe_binary}:/ytmusic_ready_check:ro\""));
    assert!(run_script.contains("ubuntu:24.04"));
    assert!(run_script.contains("/ytmusic_ready_check \"${endpoint}\""));
    assert!(run_script.contains("wait_for_ytmusic_grpc"));
    assert!(run_script.contains("wait_for_ytmusic_grpc \"http://${ytmusic_container_name}:50051\""));
    assert!(run_script.contains("Timed out waiting for ytmusic-service gRPC readiness"));
    assert!(!run_script.contains("wait_for_ytmusic_grpc \"http://127.0.0.1:50051\""));
    assert!(!run_script.contains("wait_for_port 50051 \"ytmusic-service public gRPC listener\""));
    assert!(!run_script.contains("podman "));
}

#[test]
fn fake_peer_ci_runs_a_single_mock_gate() {
    let workflow = fs::read_to_string("../../.github/workflows/fake-peer-ci.yml")
        .expect("fake-peer workflow should exist");

    assert!(workflow.contains("cargo fmt --all --check"));
    assert!(
        workflow.contains("cargo clippy --workspace --all-targets --all-features -- -D warnings")
    );
    assert_eq!(workflow.matches("cargo test --workspace -v").count(), 1);
    assert!(!workflow.contains("Run fake-peer critical subset"));
}

#[test]
fn live_confidence_workflow_exists() {
    let workflow = fs::read_to_string("../../.github/workflows/live-confidence.yml")
        .expect("live-confidence workflow should exist");

    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("workflow_dispatch:"));
    assert!(workflow.contains("uses: ./.github/workflows/live-staging.yml"));
}

#[test]
fn release_workflow_builds_native_arch_images_before_live_validation() {
    let workflow = fs::read_to_string("../../.github/workflows/release-image.yml")
        .expect("release-image workflow should exist");

    assert!(workflow.contains("prepare:"));
    assert!(workflow.contains("build-amd64:"));
    assert!(workflow.contains("build-arm64:"));
    assert!(workflow.contains("publish-candidate-manifest:"));
    assert!(workflow.contains("publish-release-tags:"));
    assert!(workflow.contains("runs-on: ubuntu-24.04-arm"));
    assert!(workflow.contains("docker/setup-buildx-action@v4"));
    assert!(workflow.contains("docker/build-push-action@v7"));
    assert!(workflow.contains("docker buildx imagetools create"));
    assert!(workflow.contains("discord_voice_service_image_ref: ${{ needs.publish-candidate-manifest.outputs.candidate_ref }}"));
    assert!(workflow.contains("discord_voice_service_image_digest: ${{ needs.publish-candidate-manifest.outputs.candidate_digest }}"));
    assert!(!workflow.contains("redhat-actions/buildah-build@v2"));
}

#[test]
fn release_workflow_encodes_stable_and_prerelease_tag_policy() {
    let workflow = fs::read_to_string("../../.github/workflows/release-image.yml")
        .expect("release-image workflow should exist");

    assert!(workflow.contains("^v([0-9]+)\\.([0-9]+)\\.([0-9]+)$"));
    assert!(workflow.contains("^v([0-9]+)\\.([0-9]+)\\.([0-9]+)-"));
    assert!(workflow.contains("latest"));
    assert!(workflow.contains("candidate-"));
}
