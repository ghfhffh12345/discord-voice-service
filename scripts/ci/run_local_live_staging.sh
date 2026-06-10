#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
env_file="${repo_root}/.env"
browser_json_file="${repo_root}/browser.json"
service_log="${RUNNER_TEMP:-/tmp}/discord-voice-service-local.log"
network_name="${LOCAL_LIVE_STAGING_NETWORK_NAME:-discord-voice-local-live-staging}"
ytmusic_container_name="${LOCAL_YTMUSIC_CONTAINER_NAME:-ytmusic-service-local-live-staging}"
service_container_name="${LOCAL_DISCORD_VOICE_SERVICE_CONTAINER_NAME:-discord-voice-service-local-live-staging}"
cpu_contention_container_name="${LOCAL_CPU_CONTENTION_CONTAINER_NAME:-discord-voice-local-live-staging-cpu-contention}"
service_container_image="${LOCAL_LIVE_STAGING_SERVICE_CONTAINER_IMAGE:-ubuntu:24.04}"
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
target_dir="${CARGO_TARGET_DIR:-${repo_root}/target}"
if [[ "${target_dir}" != /* ]]; then
  target_dir="${repo_root}/${target_dir}"
fi
service_binary="${target_dir}/debug/discord-voice-service"
ytmusic_probe_binary="${target_dir}/debug/ytmusic_ready_check"
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
service_uri="${DISCORD_VOICE_SERVICE_URI:-http://127.0.0.1:55051}"
live_staging_profile="${LIVE_STAGING_PROFILE:-constrained-local}"
live_staging_service_cpus="${LIVE_STAGING_SERVICE_CPUS:-1.0}"
live_staging_cpu_contention_workers="${LIVE_STAGING_CPU_CONTENTION_WORKERS:-2}"
live_staging_http_read_delay_ms="${LIVE_STAGING_HTTP_READ_DELAY_MS:-5}"
live_staging_http_read_jitter_ms="${LIVE_STAGING_HTTP_READ_JITTER_MS:-25}"

if [[ "${live_staging_profile}" != "constrained-local" ]]; then
  echo "LIVE_STAGING_PROFILE must be constrained-local for run_local_live_staging.sh; got ${live_staging_profile}" >&2
  exit 1
fi

unset APPLICATION_ID BOT_TOKEN OBSERVER_BOT_TOKEN TEST_GUILD_ID TEST_VOICE_CHANNEL_ID TEST_VIDEO_ID
unset DISCORD_VOICE_SERVICE_BIND_ADDR DISCORD_VOICE_SERVICE_URI DISCORD_VOICE_SERVICE_YTMUSIC_ADDR

printf '{"outcome":"failure","service_uri":"%s","ytmusic_addr":"%s","test_video_id":"%s","expected_track_duration_ms":0,"active_validation_duration_after_resume_ms":0,"pause_silence_packet_count":0,"pause_silence_spacing_ms":[],"live_staging_profile":"%s","live_staging_service_cpus":"%s","live_staging_cpu_contention_workers":%s,"live_staging_http_read_delay_ms":%s,"live_staging_http_read_jitter_ms":%s,"validated_join_voice":false,"validated_update_voice_context":false,"validated_play":false,"validated_pause":false,"validated_resume":false,"validated_invalid_resume_ignored":false,"validated_redundant_pause_ignored":false,"observer_proved_pause":false,"observer_proved_resume":false,"observer_pause_self_mute_observed":false,"observer_pause_speaking_stopped":false,"observer_pause_rtp_silence_observed":false,"observer_resume_speaking_started":false,"observer_pause_silence_ms":0,"observer_resume_packet_count":0,"validated_reconnect_rollover_during_playback":false,"validated_stop":false,"validated_stop_during_playback":false,"validated_leave_voice":false,"validated_leave_voice_during_playback":false,"validated_get_state":false,"validated_get_playback_metrics":false,"validated_subscribe_events":false,"saw_voice_connecting":false,"saw_voice_ready":false,"saw_track_resolving":false,"saw_playing":false,"saw_track_ended":false,"observed_packet_count":0,"decoded_audio_ms":0,"observer_wall_clock_elapsed_ms":0,"observer_decoded_audio_to_wall_clock_ratio_ppm":0,"non_silent_audio_ms":0,"observer_rtp_inter_arrival":{"samples":0,"p50_ms":0,"p95_ms":0,"p99_ms":0,"min_ms":0,"max_ms":0},"observer_rtp_gap_count_gte_100ms":0,"observer_rtp_fast_interval_count":0,"observer_rtp_fast_interval_min_ms":0,"observer_rtp_fast_interval_min_us":0,"observer_rtp_buffering_event_count":0,"observer_rtp_buffering_total_us":0,"observer_rtp_buffering_max_us":0,"observer_rtp_speed_change_total_abs_us":0,"observer_rtp_speed_change_total_fast_us":0,"observer_rtp_speed_change_total_slow_us":0,"observer_anomaly_count":0,"observer_anomalies":[],"observer_decoded_audio_tempo_window_count":0,"observer_decoded_audio_tempo_window_post_source_buffer_count":0,"observer_decoded_audio_tempo_window_min_ratio_ppm":0,"observer_decoded_audio_tempo_window_max_ratio_ppm":0,"observer_decoded_audio_tempo_window_fast_count":0,"observer_decoded_audio_tempo_window_fastest_ratio_ppm":0,"observer_decoded_audio_tempo_window_fastest_media_ms":0,"observer_decoded_audio_tempo_window_fastest_wall_clock_us":0,"observer_decoded_audio_tempo_window_slow_count":0,"observer_decoded_audio_tempo_window_slowest_ratio_ppm":0,"observer_decoded_audio_tempo_window_slowest_media_ms":0,"observer_decoded_audio_tempo_window_slowest_wall_clock_us":0,"observer_decoded_audio_short_tempo_window_count":0,"observer_decoded_audio_short_tempo_window_fast_count":0,"observer_decoded_audio_short_tempo_window_slow_count":0,"observer_decoded_audio_short_tempo_window_fastest":null,"observer_decoded_audio_short_tempo_window_slowest":null,"dave_transition_count_during_playback":0,"playback_metrics":null,"reconnect_probe_metrics":null,"validated_constrained_profile":false,"validated_slow_jittery_http":false,"failure_reason":"controller_not_started"}\n' \
  "${service_uri}" \
  "${service_ytmusic_addr}" \
  "${test_video_id}" \
  "${live_staging_profile}" \
  "${live_staging_service_cpus}" \
  "${live_staging_cpu_contention_workers}" \
  "${live_staging_http_read_delay_ms}" \
  "${live_staging_http_read_jitter_ms}" >"${validation_evidence_path}"

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
if [[ -n "${RUST_LOG:-}" ]]; then
  runtime_env+=(RUST_LOG="${RUST_LOG}")
fi
if [[ -n "${RUST_BACKTRACE:-}" ]]; then
  runtime_env+=(RUST_BACKTRACE="${RUST_BACKTRACE}")
fi
service_runtime_env=()
if [[ -n "${RUST_LOG:-}" ]]; then
  service_runtime_env+=(-e RUST_LOG="${RUST_LOG}")
fi
if [[ -n "${RUST_BACKTRACE:-}" ]]; then
  service_runtime_env+=(-e RUST_BACKTRACE="${RUST_BACKTRACE}")
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

  kill "${service_log_pid:-}" >/dev/null 2>&1 || true
  docker rm -f "${service_container_name}" >/dev/null 2>&1 || true
  docker rm -f "${cpu_contention_container_name}" >/dev/null 2>&1 || true
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

docker rm -f "${cpu_contention_container_name}" >/dev/null 2>&1 || true
docker rm -f "${service_container_name}" >/dev/null 2>&1 || true
docker rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true
if ! docker network inspect "${network_name}" >/dev/null 2>&1; then
  docker network create "${network_name}" >/dev/null
fi
docker pull "${ytmusic_image_ref}"
docker pull "${service_container_image}"
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

docker run -d \
  --name "${cpu_contention_container_name}" \
  --network "${network_name}" \
  -e LIVE_STAGING_CPU_CONTENTION_WORKERS="${live_staging_cpu_contention_workers}" \
  ubuntu:24.04 \
  bash -lc 'for _ in $(seq 1 "${LIVE_STAGING_CPU_CONTENTION_WORKERS}"); do yes >/dev/null & done; wait'

# Run the locally built service under a Docker CPU quota so the constrained
# profile applies to the media scheduler process itself.
docker run -d \
  --name "${service_container_name}" \
  --network host \
  --cpus "${live_staging_service_cpus}" \
  "${service_runtime_env[@]}" \
  -e DISCORD_VOICE_SERVICE_BIND_ADDR="0.0.0.0:${probe_port}" \
  -e DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="${service_ytmusic_addr}" \
  -e DISCORD_VOICE_SERVICE_HTTP_READ_DELAY_MS="${live_staging_http_read_delay_ms}" \
  -e DISCORD_VOICE_SERVICE_HTTP_READ_JITTER_MS="${live_staging_http_read_jitter_ms}" \
  -v "${service_binary}:/discord-voice-service:ro" \
  -v /etc/ssl/certs:/etc/ssl/certs:ro \
  "${service_container_image}" \
  /discord-voice-service
docker logs -f "${service_container_name}" >"${service_log}" 2>&1 &
service_log_pid=$!

attempt=0
while [[ "${attempt}" -lt 30 ]]; do
  if [[ "$(docker inspect -f '{{.State.Running}}' "${service_container_name}" 2>/dev/null || true)" != "true" ]]; then
    echo "discord-voice-service exited before readiness" >&2
    docker logs "${service_container_name}" >&2 || true
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
    LIVE_STAGING_PROFILE="${live_staging_profile}" \
    LIVE_STAGING_SERVICE_CPUS="${live_staging_service_cpus}" \
    LIVE_STAGING_CPU_CONTENTION_WORKERS="${live_staging_cpu_contention_workers}" \
    LIVE_STAGING_HTTP_READ_DELAY_MS="${live_staging_http_read_delay_ms}" \
    LIVE_STAGING_HTTP_READ_JITTER_MS="${live_staging_http_read_jitter_ms}" \
    LIVE_VALIDATION_EVIDENCE_PATH="${validation_evidence_path}" \
    cargo run -p discord-voice-service-live-validation --bin staging_live_check >"${controller_log}" 2>&1
    exit 0
  fi
  attempt=$((attempt + 1))
  "${runtime_env[@]}" sleep 1
done

echo "Timed out waiting for local discord-voice-service on ${probe_host}:${probe_port}" >&2
"${runtime_env[@]}" tail -n 200 "${service_log}" >&2 || true
exit 1
