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
set -a
source "${env_file}"
set +a

browser_json="$(cat "${browser_json_file}")"
export DISCORD_VOICE_SERVICE_BIND_ADDR="${DISCORD_VOICE_SERVICE_BIND_ADDR:-127.0.0.1:55051}"
export DISCORD_VOICE_SERVICE_URI="${DISCORD_VOICE_SERVICE_URI:-http://127.0.0.1:55051}"

if [[ ! "${DISCORD_VOICE_SERVICE_URI}" =~ ^http://([^/:]+):([0-9]+)$ ]]; then
  echo "Unsupported DISCORD_VOICE_SERVICE_URI: ${DISCORD_VOICE_SERVICE_URI}. Expected http://host:port" >&2
  exit 1
fi

probe_host="${BASH_REMATCH[1]}"
probe_port="${BASH_REMATCH[2]}"

cargo build -p discord-voice-service -p discord-voice-service-live-validation --bin staging_live_check

BROWSER_JSON="${browser_json}" cargo run -p discord-voice-service >"${service_log}" 2>&1 &
service_pid=$!
trap 'kill "${service_pid}" >/dev/null 2>&1 || true' EXIT
unset browser_json

for _ in $(seq 1 30); do
  if bash -lc "</dev/tcp/${probe_host}/${probe_port}" >/dev/null 2>&1; then
    cargo run -p discord-voice-service-live-validation --bin staging_live_check
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for local discord-voice-service on ${probe_host}:${probe_port}" >&2
tail -n 200 "${service_log}" >&2 || true
exit 1
