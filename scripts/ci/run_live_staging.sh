#!/usr/bin/env bash
set -euo pipefail

network_name="discord-voice-live-staging"
ytmusic_container_name="ytmusic-service-live-staging"
service_container_name="discord-voice-service-live-staging"
controller_log="${RUNNER_TEMP}/staging-live-check.log"
validation_evidence_path="${LIVE_VALIDATION_EVIDENCE_PATH:-${RUNNER_TEMP}/live-validation-evidence.json}"
controller_binary="${GITHUB_WORKSPACE}/target/debug/staging_live_check"
ytmusic_probe_binary="${GITHUB_WORKSPACE}/target/debug/ytmusic_ready_check"
staged_browser_json="${GITHUB_WORKSPACE}/browser.json"
service_image_ref="${DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF:-${DISCORD_VOICE_SERVICE_IMAGE_REF}}"

wait_for_port() {
  local port="$1"
  local label="$2"
  local attempts="${3:-60}"

  for _ in $(seq 1 "${attempts}"); do
    if bash -lc "</dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "::error::Timed out waiting for ${label} on 127.0.0.1:${port}"
  return 1
}

wait_for_ytmusic_grpc() {
  local endpoint="$1"
  local attempts="${2:-30}"

  for _ in $(seq 1 "${attempts}"); do
    if docker run --rm --network "${network_name}" \
      -v "${ytmusic_probe_binary}:/ytmusic_ready_check:ro" \
      ubuntu:24.04 \
      /ytmusic_ready_check "${endpoint}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "::group::ytmusic-service container log"
  docker logs "${ytmusic_container_name}" || true
  echo "::endgroup::"
  echo "::error::Timed out waiting for ytmusic-service gRPC readiness at ${endpoint}"
  return 1
}

cleanup() {
  local status=$?

  if [[ "${status}" -ne 0 ]]; then
    echo "::group::discord-voice-service container log"
    docker logs "${service_container_name}" || true
    echo "::endgroup::"

    echo "::group::staging_live_check log"
    if [[ -f "${controller_log}" ]]; then
      tail -n 200 "${controller_log}" || true
    else
      echo "controller log was not created"
    fi
    echo "::endgroup::"

    echo "::group::staging_live_check evidence"
    if [[ -f "${validation_evidence_path}" ]]; then
      cat "${validation_evidence_path}" || true
    else
      echo "validation evidence artifact was not created"
    fi
    echo "::endgroup::"

    echo "::group::ytmusic-service container log"
    docker logs "${ytmusic_container_name}" || true
    echo "::endgroup::"
  fi

  docker rm -f "${service_container_name}" >/dev/null 2>&1 || true
  docker rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true
  docker network rm "${network_name}" >/dev/null 2>&1 || true
  rm -f "${staged_browser_json}"
}

trap cleanup EXIT

install -m 644 /dev/null "${staged_browser_json}"
printf '%s' "${BROWSER_JSON}" > "${staged_browser_json}"
cat > "${validation_evidence_path}" <<EOF
{"outcome":"failure","service_uri":"${DISCORD_VOICE_SERVICE_URI}","ytmusic_addr":"${DISCORD_VOICE_SERVICE_YTMUSIC_ADDR}","saw_voice_ready":false,"saw_playing":false,"saw_track_ended":false,"observed_packet_count":0,"decoded_audio_ms":0,"non_silent_audio_ms":0,"failure_reason":"controller_not_started"}
EOF

cargo build --locked -p discord-voice-service-live-validation --bin staging_live_check --bin ytmusic_ready_check

if ! docker network inspect "${network_name}" >/dev/null 2>&1; then
  docker network create "${network_name}" >/dev/null
fi

docker rm -f "${service_container_name}" >/dev/null 2>&1 || true
docker rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true

docker pull "${YTMUSIC_SERVICE_IMAGE_REF}"
docker pull "${service_image_ref}"

docker run -d \
  --name "${ytmusic_container_name}" \
  --network "${network_name}" \
  --network-alias "${ytmusic_container_name}" \
  -p 50051:50051 \
  -p 50052:50052 \
  -e YTMUSIC_SERVICE_PUBLIC_ADDR="${YTMUSIC_SERVICE_PUBLIC_ADDR}" \
  -e YTMUSIC_SERVICE_ADMIN_ADDR="${YTMUSIC_SERVICE_ADMIN_ADDR}" \
  -e YTMUSIC_SERVICE_BROWSER_JSON="${YTMUSIC_SERVICE_BROWSER_JSON}" \
  -v "${staged_browser_json}:${YTMUSIC_SERVICE_BROWSER_JSON}:ro" \
  "${YTMUSIC_SERVICE_IMAGE_REF}"

wait_for_ytmusic_grpc "http://${ytmusic_container_name}:50051"

docker run -d \
  --name "${service_container_name}" \
  --network "${network_name}" \
  -p 55051:55051 \
  -e DISCORD_VOICE_SERVICE_BIND_ADDR="${DISCORD_VOICE_SERVICE_BIND_ADDR}" \
  -e DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="http://ytmusic-service-live-staging:50051" \
  "${service_image_ref}"

wait_for_port 55051 "discord-voice-service gRPC listener"
sleep 5

"${controller_binary}" >"${controller_log}" 2>&1

if [[ ! -s "${validation_evidence_path}" ]]; then
  echo "::error::Live validation evidence artifact was empty"
  exit 1
fi
