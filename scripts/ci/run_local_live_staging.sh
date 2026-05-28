#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
env_file="${repo_root}/.env"
browser_json_file="${repo_root}/browser.json"
service_log="${RUNNER_TEMP:-/tmp}/discord-voice-service-local.log"
network_name="${LOCAL_LIVE_STAGING_NETWORK_NAME:-discord-voice-local-live-staging}"
ytmusic_container_name="${LOCAL_YTMUSIC_CONTAINER_NAME:-ytmusic-service-local-live-staging}"
ytmusic_public_port="${LOCAL_YTMUSIC_SERVICE_PUBLIC_PORT:-50051}"
ytmusic_admin_port="${LOCAL_YTMUSIC_SERVICE_ADMIN_PORT:-50052}"
ytmusic_public_addr="${YTMUSIC_SERVICE_PUBLIC_ADDR:-0.0.0.0:50051}"
ytmusic_admin_addr="${YTMUSIC_SERVICE_ADMIN_ADDR:-0.0.0.0:50052}"
ytmusic_browser_json="${YTMUSIC_SERVICE_BROWSER_JSON:-/run/secrets/browser.json}"
host_ytmusic_endpoint="http://127.0.0.1:${ytmusic_public_port}"
service_ytmusic_addr="http://127.0.0.1:${ytmusic_public_port}"
controller_log="${RUNNER_TEMP:-/tmp}/staging-live-check-local.log"
validation_evidence_path="${LIVE_VALIDATION_EVIDENCE_PATH:-${RUNNER_TEMP:-/tmp}/live-validation-evidence-local.json}"

if [[ ! -f "${env_file}" ]]; then
  echo ".env not found at ${env_file}" >&2
  exit 1
fi

if [[ ! -f "${browser_json_file}" ]]; then
  echo "browser.json not found at ${browser_json_file}" >&2
  exit 1
fi

cd "${repo_root}"
ytmusic_probe_binary="${CARGO_TARGET_DIR:-${repo_root}/target}/debug/ytmusic_ready_check"
cargo build -p discord-voice-service --bin discord-voice-service
cargo build -p discord-voice-service-live-validation --bin staging_live_check --bin ytmusic_ready_check

source "${env_file}"
ytmusic_image_ref="${YTMUSIC_SERVICE_IMAGE_REF:-ghcr.io/ghfhffh12345/ytmusic-service:latest}"
cat "${browser_json_file}" >/dev/null
if [[ ! -s "${browser_json_file}" ]]; then
  echo "browser.json at ${browser_json_file} was empty" >&2
  exit 1
fi

application_id="${APPLICATION_ID:?APPLICATION_ID must be set in ${env_file}}"
bot_token="${BOT_TOKEN:?BOT_TOKEN must be set in ${env_file}}"
observer_bot_token="${OBSERVER_BOT_TOKEN:?OBSERVER_BOT_TOKEN must be set in ${env_file}}"
test_guild_id="${TEST_GUILD_ID:?TEST_GUILD_ID must be set in ${env_file}}"
test_voice_channel_id="${TEST_VOICE_CHANNEL_ID:?TEST_VOICE_CHANNEL_ID must be set in ${env_file}}"
test_video_id="${TEST_VIDEO_ID:?TEST_VIDEO_ID must be set in ${env_file}}"
service_bind_addr="${DISCORD_VOICE_SERVICE_BIND_ADDR:-127.0.0.1:55051}"
service_uri="${DISCORD_VOICE_SERVICE_URI:-http://127.0.0.1:55051}"

unset APPLICATION_ID BOT_TOKEN OBSERVER_BOT_TOKEN TEST_GUILD_ID TEST_VOICE_CHANNEL_ID TEST_VIDEO_ID
unset DISCORD_VOICE_SERVICE_BIND_ADDR DISCORD_VOICE_SERVICE_URI DISCORD_VOICE_SERVICE_YTMUSIC_ADDR

printf '{"outcome":"failure","service_uri":"%s","ytmusic_addr":"%s","saw_voice_ready":false,"saw_playing":false,"saw_track_ended":false,"observed_packet_count":0,"decoded_audio_ms":0,"non_silent_audio_ms":0,"failure_reason":"controller_not_started"}\n' \
  "${service_uri}" \
  "${service_ytmusic_addr}" >"${validation_evidence_path}"

runtime_env=(env -i PATH="${PATH}")
if [[ -n "${HOME:-}" ]]; then
  runtime_env+=(HOME="${HOME}")
fi
if [[ -n "${CARGO_HOME:-}" ]]; then
  runtime_env+=(CARGO_HOME="${CARGO_HOME}")
fi
if [[ -n "${RUSTUP_HOME:-}" ]]; then
  runtime_env+=(RUSTUP_HOME="${RUSTUP_HOME}")
fi

wait_for_ytmusic_grpc() {
  local endpoint="$1"
  local attempts="${2:-30}"
  local attempt=0

  while [[ "${attempt}" -lt "${attempts}" ]]; do
    if "${runtime_env[@]}" "${ytmusic_probe_binary}" "${endpoint}" >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    "${runtime_env[@]}" sleep 1
  done

  echo "Timed out waiting for helper-managed ytmusic-service gRPC readiness at ${endpoint}" >&2
  return 1
}

cleanup() {
  local exit_code=$?

  if [[ "${exit_code}" -ne 0 ]]; then
    echo "::group::discord-voice-service local log tail" >&2
    "${runtime_env[@]}" tail -n 200 "${service_log}" >&2 || true
    echo "::endgroup::" >&2

    echo "::group::staging_live_check controller log tail" >&2
    "${runtime_env[@]}" tail -n 200 "${controller_log}" >&2 || true
    echo "::endgroup::" >&2

    echo "::group::live validation evidence" >&2
    "${runtime_env[@]}" cat "${validation_evidence_path}" >&2 || true
    echo >&2
    echo "::endgroup::" >&2

    echo "::group::ytmusic-service container logs" >&2
    docker logs "${ytmusic_container_name}" || true
    echo "::endgroup::" >&2
  fi

  kill "${service_pid:-}" >/dev/null 2>&1 || true
  docker rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true
  docker network rm "${network_name}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

if [[ ! "${service_uri}" =~ ^http://([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9.-]*[A-Za-z0-9]):([0-9]+)$ ]]; then
  echo "Unsupported DISCORD_VOICE_SERVICE_URI: ${service_uri}. Expected http://host:port" >&2
  exit 1
fi

probe_host="${BASH_REMATCH[1]}"
probe_port="${BASH_REMATCH[2]}"

docker rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true
if ! docker network inspect "${network_name}" >/dev/null 2>&1; then
  docker network create "${network_name}" >/dev/null
fi
docker pull "${ytmusic_image_ref}"
docker run -d \
  --name "${ytmusic_container_name}" \
  --network "${network_name}" \
  --network-alias "${ytmusic_container_name}" \
  -p "${ytmusic_public_port}:50051" \
  -p "${ytmusic_admin_port}:50052" \
  -e YTMUSIC_SERVICE_PUBLIC_ADDR="${ytmusic_public_addr}" \
  -e YTMUSIC_SERVICE_ADMIN_ADDR="${ytmusic_admin_addr}" \
  -e YTMUSIC_SERVICE_BROWSER_JSON="${ytmusic_browser_json}" \
  -v "${browser_json_file}:${ytmusic_browser_json}:ro" \
  "${ytmusic_image_ref}"
wait_for_ytmusic_grpc "${host_ytmusic_endpoint}"

"${runtime_env[@]}" \
DISCORD_VOICE_SERVICE_BIND_ADDR="${service_bind_addr}" \
DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="${service_ytmusic_addr}" \
cargo run -p discord-voice-service >"${service_log}" 2>&1 &
service_pid=$!

attempt=0
while [[ "${attempt}" -lt 30 ]]; do
  if ! kill -0 "${service_pid}" >/dev/null 2>&1; then
    echo "discord-voice-service exited before readiness" >&2
    "${runtime_env[@]}" tail -n 200 "${service_log}" >&2 || true
    exit 1
  fi

  if "${runtime_env[@]}" bash -lc "</dev/tcp/${probe_host}/${probe_port}" >/dev/null 2>&1; then
    "${runtime_env[@]}" \
    APPLICATION_ID="${application_id}" \
    BOT_TOKEN="${bot_token}" \
    OBSERVER_BOT_TOKEN="${observer_bot_token}" \
    TEST_GUILD_ID="${test_guild_id}" \
    TEST_VOICE_CHANNEL_ID="${test_voice_channel_id}" \
    TEST_VIDEO_ID="${test_video_id}" \
    DISCORD_VOICE_SERVICE_URI="${service_uri}" \
    DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="${service_ytmusic_addr}" \
    LIVE_VALIDATION_EVIDENCE_PATH="${validation_evidence_path}" \
    cargo run -p discord-voice-service-live-validation --bin staging_live_check > /dev/null 2>"${controller_log}"
    exit 0
  fi
  attempt=$((attempt + 1))
  "${runtime_env[@]}" sleep 1
done

echo "Timed out waiting for local discord-voice-service on ${probe_host}:${probe_port}" >&2
"${runtime_env[@]}" tail -n 200 "${service_log}" >&2 || true
exit 1
