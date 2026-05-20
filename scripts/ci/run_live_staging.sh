#!/usr/bin/env bash
set -euo pipefail

network_name="discord-voice-live-staging"
ytmusic_container_name="ytmusic-service-live-staging"
service_container_name="discord-voice-service-live-staging"
controller_log="${RUNNER_TEMP}/staging-live-check.log"
controller_binary="${GITHUB_WORKSPACE}/target/debug/staging_live_check"
staged_browser_json="${GITHUB_WORKSPACE}/browser.json"

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

cleanup() {
  local status=$?

  if [[ "${status}" -ne 0 ]]; then
    echo "::group::discord-voice-service container log"
    podman logs "${service_container_name}" || true
    echo "::endgroup::"

    echo "::group::staging_live_check log"
    if [[ -f "${controller_log}" ]]; then
      tail -n 200 "${controller_log}" || true
    else
      echo "controller log was not created"
    fi
    echo "::endgroup::"

    echo "::group::ytmusic-service container log"
    podman logs "${ytmusic_container_name}" || true
    echo "::endgroup::"
  fi

  podman rm -f "${service_container_name}" >/dev/null 2>&1 || true
  podman rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true
  podman network rm "${network_name}" >/dev/null 2>&1 || true
  rm -f "${staged_browser_json}"
}

trap cleanup EXIT

browser_json_source_path="${BROWSER_JSON_SOURCE_PATH:-${STAGING_BROWSER_JSON_SOURCE_PATH:-}}"
install -m 600 "${browser_json_source_path}" "${staged_browser_json}"

if ! podman network inspect "${network_name}" >/dev/null 2>&1; then
  podman network create "${network_name}" >/dev/null
fi

podman rm -f "${service_container_name}" >/dev/null 2>&1 || true
podman rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true

podman pull "${DISCORD_VOICE_SERVICE_IMAGE_REF}"

podman run -d \
  --name "${ytmusic_container_name}" \
  --network "${network_name}" \
  --network-alias "${ytmusic_container_name}" \
  -p 50051:50051 \
  -p 50052:50052 \
  -e YTMUSIC_SERVICE_PUBLIC_ADDR="${YTMUSIC_SERVICE_PUBLIC_ADDR}" \
  -e YTMUSIC_SERVICE_ADMIN_ADDR="${YTMUSIC_SERVICE_ADMIN_ADDR}" \
  -e YTMUSIC_SERVICE_BROWSER_JSON="${YTMUSIC_SERVICE_BROWSER_JSON}" \
  -v "${staged_browser_json}:${YTMUSIC_SERVICE_BROWSER_JSON}:ro,Z" \
  "${YTMUSIC_SERVICE_IMAGE_REF}"

wait_for_port 50051 "ytmusic-service public gRPC listener"

podman run -d \
  --name "${service_container_name}" \
  --network "${network_name}" \
  -p 55051:55051 \
  -e DISCORD_VOICE_SERVICE_BIND_ADDR="0.0.0.0:55051" \
  -e DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="http://ytmusic-service-live-staging:50051" \
  "${DISCORD_VOICE_SERVICE_IMAGE_REF}"

wait_for_port 55051 "discord-voice-service gRPC listener"
sleep 5

cargo build --locked --bin staging_live_check
"${controller_binary}" >"${controller_log}" 2>&1
