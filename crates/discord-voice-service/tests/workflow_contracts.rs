use std::fs;

const OCCUPIED_LISTENER_CONTRACT: &str = "During live staging, human listeners may remain in the channel while the staging bot validates playback against the short dedicated validation track.";
const NATURAL_END_SUCCESS_CONTRACT: &str = "Live-staging success waits for the natural end of the validation track before the run is treated as release-ready.";
const LOCAL_LIVE_STAGING_CONTRACT: &str = "For local real-Discord live staging, run `scripts/ci/run_local_live_staging.sh`; the helper loads secrets from `.env`, loads `BROWSER_JSON` from `./browser.json`, starts a disposable local `ytmusic-service` container, waits for its gRPC readiness, then starts a source-built `discord-voice-service` before running observer validation.";
const OBSERVER_SECRET_CONTRACT: &str = "Protected live staging requires `OBSERVER_BOT_TOKEN` for the muted, non-deafened observer identity that validates receive-side audio.";
const RECEIVE_SIDE_SUCCESS_CONTRACT: &str = "Live-staging success requires observer receive-side proof: authentic voice context, VoiceReady, Playing, natural TrackEnded, at least 120 observed packets, at least 3000 ms decoded audio, at least 1000 ms non-silent audio, and no reconnect/interruption/fatal error during validation.";
const EVIDENCE_ARTIFACT_CONTRACT: &str = "Live-staging always uploads a structured observer evidence artifact summarizing observed packets, decoded audio, non-silent audio, and failure_reason.";

#[test]
fn live_staging_workflow_uses_github_hosted_runner_and_secret_browser_json() {
    let _ = (
        LOCAL_LIVE_STAGING_CONTRACT,
        OBSERVER_SECRET_CONTRACT,
        RECEIVE_SIDE_SUCCESS_CONTRACT,
        EVIDENCE_ARTIFACT_CONTRACT,
    );
    let workflow = fs::read_to_string("../../.github/workflows/live-staging.yml")
        .expect("live-staging workflow should exist");
    let preflight = fs::read_to_string("../../scripts/ci/live_staging_preflight.sh")
        .expect("live staging preflight script should exist");
    let run_script = fs::read_to_string("../../scripts/ci/run_live_staging.sh")
        .expect("live staging runner script should exist");
    let local_helper = fs::read_to_string("../../scripts/ci/run_local_live_staging.sh")
        .expect("live staging local helper script should exist");
    let build_service = "cargo build -p discord-voice-service --bin discord-voice-service";
    let build_validator = "cargo build -p discord-voice-service-live-validation --bin staging_live_check --bin ytmusic_ready_check";
    let source_env = "source \"${env_file}\"";
    let local_ytmusic_probe = "ytmusic_probe_binary=\"${CARGO_TARGET_DIR:-${repo_root}/target}/debug/ytmusic_ready_check\"";
    let local_ytmusic_image = "ytmusic_image_ref=\"${YTMUSIC_SERVICE_IMAGE_REF:-ghcr.io/ghfhffh12345/ytmusic-service:latest}\"";
    let local_ytmusic_run = "docker run -d \\\n  --name \"${ytmusic_container_name}\"";
    let local_ytmusic_pull = "docker pull \"${ytmusic_image_ref}\"";
    let local_network_create = "docker network create \"${network_name}\"";
    let local_evidence_default = "validation_evidence_path=\"${LIVE_VALIDATION_EVIDENCE_PATH:-${RUNNER_TEMP:-/tmp}/live-validation-evidence-local.json}\"";
    let local_wait_for_ytmusic = "wait_for_ytmusic_grpc \"${host_ytmusic_endpoint}\"";
    let local_service_start = "cargo run -p discord-voice-service >\"${service_log}\" 2>&1 &";

    assert!(workflow.contains("runs-on: ubuntu-24.04"));
    assert!(workflow.contains("environment: live-staging"));
    assert!(workflow.contains("BROWSER_JSON: ${{ secrets.BROWSER_JSON }}"));
    assert!(workflow.contains("OBSERVER_BOT_TOKEN: ${{ secrets.OBSERVER_BOT_TOKEN }}"));
    assert!(!workflow.contains("OBSERVER_APPLICATION_ID"));
    assert!(workflow.contains("scripts/ci/live_staging_preflight.sh"));
    assert!(workflow.contains("scripts/ci/run_live_staging.sh"));
    assert!(workflow.contains("DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF"));
    assert!(workflow.contains("${DISCORD_VOICE_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(workflow.contains("${YTMUSIC_SERVICE_IMAGE_REF:-not resolved}"));
    assert!(workflow.contains("Validation mode: observer receive-side live contract"));
    assert!(workflow.contains("structured observer evidence artifact"));
    assert!(workflow.contains("actions/upload-artifact"));
    assert!(workflow.contains("packages: read"));
    assert!(workflow.contains("docker/login-action@v3"));
    assert!(!workflow.contains("self-hosted"));
    assert!(!workflow.contains("discord-voice-staging"));
    assert!(!workflow.contains("STAGING_BROWSER_JSON_SOURCE_PATH"));

    assert!(preflight.contains("APPLICATION_ID"));
    assert!(preflight.contains("BOT_TOKEN"));
    assert!(preflight.contains("OBSERVER_BOT_TOKEN"));
    assert!(preflight.contains("BROWSER_JSON"));
    assert!(!preflight.contains("OBSERVER_APPLICATION_ID"));
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
    assert!(
        run_script.contains("wait_for_ytmusic_grpc \"http://${ytmusic_container_name}:50051\"")
    );
    assert!(run_script.contains("Timed out waiting for ytmusic-service gRPC readiness"));
    assert!(!run_script.contains("wait_for_ytmusic_grpc \"http://127.0.0.1:50051\""));
    assert!(!run_script.contains("wait_for_port 50051 \"ytmusic-service public gRPC listener\""));
    assert!(!run_script.contains("podman "));

    assert!(local_helper.contains(source_env));
    assert!(!local_helper.contains("set -a"));
    assert!(local_helper.contains("cat \"${browser_json_file}\""));
    assert!(local_helper.contains("cat \"${browser_json_file}\" >/dev/null"));
    assert!(local_helper.contains("[[ ! -s \"${browser_json_file}\" ]]"));
    assert!(local_helper.contains(build_service));
    assert!(local_helper.contains(build_validator));
    assert!(local_helper.contains(local_ytmusic_probe));
    assert!(local_helper.find(build_service) < local_helper.find(source_env));
    assert!(local_helper.find(build_validator) < local_helper.find(source_env));
    assert!(local_helper.find(local_ytmusic_probe) < local_helper.find(source_env));
    assert!(local_helper.find(source_env) < local_helper.find(local_ytmusic_image));
    assert!(local_helper.contains("env -i"));
    assert!(local_helper.contains("PATH=\"${PATH}\""));
    assert!(local_helper.contains(local_ytmusic_image));
    assert!(local_helper.contains(local_network_create));
    assert!(local_helper.contains(local_ytmusic_pull));
    assert!(local_helper.contains(local_ytmusic_run));
    assert!(local_helper.contains("wait_for_ytmusic_grpc()"));
    assert!(local_helper.contains(
        "\"${runtime_env[@]}\" \"${ytmusic_probe_binary}\" \"${endpoint}\" >/dev/null 2>&1"
    ));
    assert!(local_helper.contains(
        "Timed out waiting for helper-managed ytmusic-service gRPC readiness at ${endpoint}"
    ));
    assert!(local_helper.contains(local_wait_for_ytmusic));
    assert!(local_helper.find(local_wait_for_ytmusic) < local_helper.find(local_service_start));
    assert!(local_helper.contains(local_service_start));
    assert!(local_helper.contains("service_pid=$!"));
    assert!(
        local_helper.contains("DISCORD_VOICE_SERVICE_YTMUSIC_ADDR=\"${service_ytmusic_addr}\"")
    );
    assert!(local_helper.contains(local_evidence_default));
    assert!(local_helper.contains("docker logs \"${ytmusic_container_name}\" || true"));
    assert!(
        local_helper.contains("docker rm -f \"${ytmusic_container_name}\" >/dev/null 2>&1 || true")
    );
    assert!(local_helper.contains("docker network rm \"${network_name}\" >/dev/null 2>&1 || true"));
    assert!(!local_helper.contains("ensure ytmusic-service is running before local live staging"));
    assert!(
        !local_helper.contains("service_ytmusic_addr=\"${DISCORD_VOICE_SERVICE_YTMUSIC_ADDR:?")
    );
    assert!(local_helper.contains("DISCORD_VOICE_SERVICE_BIND_ADDR=\"${service_bind_addr}\""));
    assert!(local_helper.contains("APPLICATION_ID=\"${application_id}\""));
    assert!(local_helper.contains("BOT_TOKEN=\"${bot_token}\""));
    assert!(local_helper.contains("OBSERVER_BOT_TOKEN=\"${observer_bot_token}\""));
    assert!(local_helper.contains("TEST_GUILD_ID=\"${test_guild_id}\""));
    assert!(local_helper.contains("TEST_VOICE_CHANNEL_ID=\"${test_voice_channel_id}\""));
    assert!(local_helper.contains("TEST_VIDEO_ID=\"${test_video_id}\""));
    assert!(local_helper.contains("DISCORD_VOICE_SERVICE_URI=\"${service_uri}\""));
    assert!(local_helper.contains("LIVE_VALIDATION_EVIDENCE_PATH=\"${validation_evidence_path}\""));
    assert!(
        local_helper.contains(
            "cargo run -p discord-voice-service-live-validation --bin staging_live_check"
        )
    );
    assert!(local_helper.contains("probe_host=\"${BASH_REMATCH[1]}\""));
    assert!(local_helper.contains("probe_port=\"${BASH_REMATCH[2]}\""));
    assert!(local_helper.contains("Unsupported DISCORD_VOICE_SERVICE_URI"));
    assert!(!local_helper.contains("(?:"));
    assert!(local_helper.contains("kill -0 \"${service_pid}\""));
    assert!(local_helper.contains("discord-voice-service exited before readiness"));
    assert!(local_helper.contains("attempt=0"));
    assert!(local_helper.contains("while [[ \"${attempt}\" -lt 30 ]]"));
    assert!(!local_helper.contains("seq 1 30"));
    assert!(local_helper.contains("\"${runtime_env[@]}\" bash -lc"));
}

#[test]
fn fake_peer_ci_runs_a_single_mock_gate() {
    let workflow = fs::read_to_string("../../.github/workflows/fake-peer-ci.yml")
        .expect("fake-peer workflow should exist");

    assert!(workflow.contains("cargo fmt --all --check"));
    assert!(
        workflow.contains("cargo clippy --workspace --all-targets --all-features -- -D warnings")
    );
    assert!(workflow.contains("RUST_TEST_THREADS: \"1\""));
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

#[test]
fn live_staging_runner_doc_matches_the_live_validation_contract() {
    let doc = fs::read_to_string("../../docs/operations/live-staging-runner.md")
        .expect("live staging runner doc should exist");

    assert!(doc.contains(OCCUPIED_LISTENER_CONTRACT));
    assert!(doc.contains(NATURAL_END_SUCCESS_CONTRACT));
    assert!(doc.contains(LOCAL_LIVE_STAGING_CONTRACT));
    assert!(doc.contains(OBSERVER_SECRET_CONTRACT));
    assert!(doc.contains(RECEIVE_SIDE_SUCCESS_CONTRACT));
    assert!(doc.contains(EVIDENCE_ARTIFACT_CONTRACT));
    assert!(!doc.contains("verifies the already-running `ytmusic-service` endpoint"));
    assert!(!doc.contains("5-second live interval"));
    assert!(!doc.contains("OBSERVER_APPLICATION_ID"));
}
