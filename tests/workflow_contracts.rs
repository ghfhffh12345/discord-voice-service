use std::fs;

#[test]
fn live_staging_workflow_targets_container_artifacts() {
    let workflow = fs::read_to_string(".github/workflows/live-staging.yml")
        .expect("live-staging workflow should exist");
    let preflight = fs::read_to_string("scripts/ci/live_staging_preflight.sh")
        .expect("live staging preflight script should exist");
    let run_script = fs::read_to_string("scripts/ci/run_live_staging.sh")
        .expect("live staging runner script should exist");

    assert!(workflow.contains("discord_voice_service_image_ref"));
    assert!(workflow.contains("DISCORD_VOICE_SERVICE_URI"));
    assert!(workflow.contains("scripts/ci/live_staging_preflight.sh"));
    assert!(workflow.contains("scripts/ci/run_live_staging.sh"));
    assert!(workflow.contains("DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF"));
    assert!(workflow.contains("${DISCORD_VOICE_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(workflow.contains("${YTMUSIC_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(
        !workflow.contains("Build local binaries"),
        "live staging should no longer build local binaries",
    );
    assert!(preflight.contains("DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF"));
    assert!(run_script.contains("service_image_ref"));
    assert!(run_script.contains("podman pull \"${service_image_ref}\""));
    assert!(run_script.contains("\"${service_image_ref}\""));
}

#[test]
fn release_workflow_promotes_validated_candidate_digest() {
    let workflow = fs::read_to_string(".github/workflows/release-image.yml")
        .expect("release-image workflow should exist");

    assert!(workflow.contains("candidate-"));
    assert!(workflow.contains("discord_voice_service_image_digest"));
    assert!(workflow.contains("scripts/ci/promote_candidate_image.sh"));
}

#[test]
fn live_confidence_workflow_exists() {
    let workflow = fs::read_to_string(".github/workflows/live-confidence.yml")
        .expect("live-confidence workflow should exist");

    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("workflow_dispatch:"));
    assert!(workflow.contains("uses: ./.github/workflows/live-staging.yml"));
}
