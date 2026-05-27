#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
env_file="${repo_root}/.env"
browser_json_file="${repo_root}/browser.json"
service_log="${RUNNER_TEMP:-/tmp}/discord-voice-service-local.log"

if [[ ! -f "${env_file}" ]]; then
  echo ".env not found at ${env_file}" >&2
  exit 1
fi

if [[ ! -f "${browser_json_file}" ]]; then
  echo "browser.json not found at ${browser_json_file}" >&2
  exit 1
fi

cd "${repo_root}"
cargo build -p discord-voice-service --bin discord-voice-service
cargo build -p discord-voice-service-live-validation --bin staging_live_check

source "${env_file}"
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
service_ytmusic_addr="${DISCORD_VOICE_SERVICE_YTMUSIC_ADDR:?DISCORD_VOICE_SERVICE_YTMUSIC_ADDR must be set in ${env_file}}"

unset APPLICATION_ID BOT_TOKEN OBSERVER_BOT_TOKEN TEST_GUILD_ID TEST_VOICE_CHANNEL_ID TEST_VIDEO_ID
unset DISCORD_VOICE_SERVICE_BIND_ADDR DISCORD_VOICE_SERVICE_URI DISCORD_VOICE_SERVICE_YTMUSIC_ADDR

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

if [[ ! "${service_uri}" =~ ^http://([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9.-]*[A-Za-z0-9]):([0-9]+)$ ]]; then
  echo "Unsupported DISCORD_VOICE_SERVICE_URI: ${service_uri}. Expected http://host:port" >&2
  exit 1
fi

probe_host="${BASH_REMATCH[1]}"
probe_port="${BASH_REMATCH[2]}"

"${runtime_env[@]}" \
DISCORD_VOICE_SERVICE_BIND_ADDR="${service_bind_addr}" \
DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="${service_ytmusic_addr}" \
cargo run -p discord-voice-service >"${service_log}" 2>&1 &
service_pid=$!
trap 'kill "${service_pid}" >/dev/null 2>&1 || true' EXIT

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
    cargo run -p discord-voice-service-live-validation --bin staging_live_check
    exit 0
  fi
  attempt=$((attempt + 1))
  "${runtime_env[@]}" sleep 1
done

echo "Timed out waiting for local discord-voice-service on ${probe_host}:${probe_port}" >&2
"${runtime_env[@]}" tail -n 200 "${service_log}" >&2 || true
exit 1
