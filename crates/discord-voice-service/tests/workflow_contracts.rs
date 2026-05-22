use std::fs;

#[test]
fn live_staging_workflow_targets_container_artifacts() {
    let workflow = fs::read_to_string("../../.github/workflows/live-staging.yml")
        .expect("live-staging workflow should exist");
    let preflight = fs::read_to_string("../../scripts/ci/live_staging_preflight.sh")
        .expect("live staging preflight script should exist");
    let run_script = fs::read_to_string("../../scripts/ci/run_live_staging.sh")
        .expect("live staging runner script should exist");

    assert!(workflow.contains("discord_voice_service_image_ref"));
    assert!(workflow.contains("DISCORD_VOICE_SERVICE_URI"));
    assert!(workflow.contains("DISCORD_VOICE_SERVICE_BIND_ADDR: 0.0.0.0:55051"));
    assert!(workflow.contains("scripts/ci/live_staging_preflight.sh"));
    assert!(workflow.contains("scripts/ci/run_live_staging.sh"));
    assert!(workflow.contains("DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF"));
    assert!(workflow.contains("${DISCORD_VOICE_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(workflow.contains("${YTMUSIC_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(workflow.contains("packages: read"));
    assert!(workflow.contains("redhat-actions/podman-login@v1"));
    assert!(workflow.contains("registry: ghcr.io"));
    assert!(
        !workflow.contains("Build local binaries"),
        "live staging should no longer build local binaries",
    );
    assert!(preflight.contains("DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF"));
    assert!(run_script.contains("service_image_ref"));
    assert!(run_script.contains("podman pull \"${service_image_ref}\""));
    assert!(
        run_script
            .contains("-e DISCORD_VOICE_SERVICE_BIND_ADDR=\"${DISCORD_VOICE_SERVICE_BIND_ADDR}\"")
    );
    assert!(run_script.contains("\"${service_image_ref}\""));
}

#[test]
fn live_staging_browser_json_is_materialized_with_container_readable_permissions() {
    let workflow = fs::read_to_string("../../.github/workflows/live-staging.yml")
        .expect("live-staging workflow should exist");
    let run_script = fs::read_to_string("../../scripts/ci/run_live_staging.sh")
        .expect("live staging runner script should exist");

    assert!(workflow.contains("scripts/ci/run_live_staging.sh"));
    assert!(
        run_script.contains("install -m 644"),
        "hosted live staging must materialize browser.json with container-readable permissions before the read-only bind mount",
    );
    assert!(
        !run_script.contains("install -m 600"),
        "hosted live staging must not regress to host-only browser.json permissions",
    );
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
