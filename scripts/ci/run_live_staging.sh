#!/usr/bin/env bash
set -euo pipefail

network_name="discord-voice-live-staging"
ytmusic_container_name="ytmusic-service-live-staging"
service_container_name="discord-voice-service-live-staging"
cpu_contention_container_name="discord-voice-live-staging-cpu-contention"
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

validate_success_evidence() {
  local evidence_path="$1"

  python3 "${GITHUB_WORKSPACE}/scripts/ci/validate_live_staging_evidence.py" "${evidence_path}"
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
  docker rm -f "${cpu_contention_container_name}" >/dev/null 2>&1 || true
  docker rm -f "${ytmusic_container_name}" >/dev/null 2>&1 || true
  docker network rm "${network_name}" >/dev/null 2>&1 || true
  rm -f "${staged_browser_json}"
}

trap cleanup EXIT

install -m 644 /dev/null "${staged_browser_json}"
printf '%s' "${BROWSER_JSON}" > "${staged_browser_json}"
cat > "${validation_evidence_path}" <<EOF
{"outcome":"failure","service_uri":"${DISCORD_VOICE_SERVICE_URI}","ytmusic_addr":"${DISCORD_VOICE_SERVICE_YTMUSIC_ADDR}","test_video_id":"${TEST_VIDEO_ID}","expected_track_duration_ms":0,"active_validation_duration_after_resume_ms":0,"pause_silence_packet_count":0,"pause_silence_spacing_ms":[],"live_staging_profile":"${LIVE_STAGING_PROFILE}","live_staging_service_cpus":"${LIVE_STAGING_SERVICE_CPUS}","live_staging_cpu_contention_workers":${LIVE_STAGING_CPU_CONTENTION_WORKERS},"live_staging_http_read_delay_ms":${LIVE_STAGING_HTTP_READ_DELAY_MS},"live_staging_http_read_jitter_ms":${LIVE_STAGING_HTTP_READ_JITTER_MS},"validated_join_voice":false,"validated_update_voice_context":false,"validated_play":false,"validated_pause":false,"validated_resume":false,"validated_invalid_resume_ignored":false,"validated_redundant_pause_ignored":false,"observer_proved_pause":false,"observer_proved_resume":false,"observer_pause_self_mute_observed":false,"observer_pause_speaking_stopped":false,"observer_pause_rtp_silence_observed":false,"observer_resume_speaking_started":false,"observer_pause_silence_ms":0,"observer_resume_packet_count":0,"validated_reconnect_rollover_during_playback":false,"validated_stop":false,"validated_stop_during_playback":false,"validated_leave_voice":false,"validated_leave_voice_during_playback":false,"validated_get_state":false,"validated_get_playback_metrics":false,"validated_subscribe_events":false,"saw_voice_connecting":false,"saw_voice_ready":false,"saw_track_resolving":false,"saw_buffering":false,"saw_playing":false,"saw_paused":false,"saw_resumed_playing":false,"saw_track_ended":false,"observed_packet_count":0,"decoded_audio_ms":0,"observer_wall_clock_elapsed_ms":0,"observer_decoded_audio_to_wall_clock_ratio_ppm":0,"non_silent_audio_ms":0,"observer_rtp_inter_arrival":{"samples":0,"p50_ms":0,"p95_ms":0,"p99_ms":0,"min_ms":0,"max_ms":0},"observer_rtp_gap_count_gte_100ms":0,"observer_rtp_fast_interval_count":0,"observer_rtp_fast_interval_min_ms":0,"observer_rtp_fast_interval_min_us":0,"observer_rtp_buffering_event_count":0,"observer_rtp_buffering_total_us":0,"observer_rtp_buffering_max_us":0,"observer_rtp_speed_change_total_abs_us":0,"observer_rtp_speed_change_total_fast_us":0,"observer_rtp_speed_change_total_slow_us":0,"observer_anomaly_count":0,"observer_anomalies":[],"observer_decoded_audio_tempo_window_count":0,"observer_decoded_audio_tempo_window_post_source_buffer_count":0,"observer_decoded_audio_tempo_window_min_ratio_ppm":0,"observer_decoded_audio_tempo_window_max_ratio_ppm":0,"observer_decoded_audio_tempo_window_fast_count":0,"observer_decoded_audio_tempo_window_fastest_ratio_ppm":0,"observer_decoded_audio_tempo_window_fastest_media_ms":0,"observer_decoded_audio_tempo_window_fastest_wall_clock_us":0,"observer_decoded_audio_tempo_window_slow_count":0,"observer_decoded_audio_tempo_window_slowest_ratio_ppm":0,"observer_decoded_audio_tempo_window_slowest_media_ms":0,"observer_decoded_audio_tempo_window_slowest_wall_clock_us":0,"observer_decoded_audio_short_tempo_window_count":0,"observer_decoded_audio_short_tempo_window_fast_count":0,"observer_decoded_audio_short_tempo_window_slow_count":0,"observer_decoded_audio_short_tempo_window_fastest":null,"observer_decoded_audio_short_tempo_window_slowest":null,"dave_transition_count_during_playback":0,"playback_metrics":null,"reconnect_probe_metrics":null,"validated_constrained_profile":false,"validated_slow_jittery_http":false,"failure_reason":"controller_not_started"}
EOF

cargo build --locked -p discord-voice-service-live-validation --bin staging_live_check --bin ytmusic_ready_check

if ! docker network inspect "${network_name}" >/dev/null 2>&1; then
  docker network create "${network_name}" >/dev/null
fi

docker rm -f "${service_container_name}" >/dev/null 2>&1 || true
docker rm -f "${cpu_contention_container_name}" >/dev/null 2>&1 || true
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
  --name "${cpu_contention_container_name}" \
  --network "${network_name}" \
  -e LIVE_STAGING_CPU_CONTENTION_WORKERS="${LIVE_STAGING_CPU_CONTENTION_WORKERS}" \
  ubuntu:24.04 \
  bash -lc 'for _ in $(seq 1 "${LIVE_STAGING_CPU_CONTENTION_WORKERS}"); do yes >/dev/null & done; wait'

docker run -d \
  --name "${service_container_name}" \
  --network "${network_name}" \
  --cpus "${LIVE_STAGING_SERVICE_CPUS}" \
  -p 55051:55051 \
  -e DISCORD_VOICE_SERVICE_BIND_ADDR="${DISCORD_VOICE_SERVICE_BIND_ADDR}" \
  -e DISCORD_VOICE_SERVICE_YTMUSIC_ADDR="http://ytmusic-service-live-staging:50051" \
  -e DISCORD_VOICE_SERVICE_HTTP_READ_DELAY_MS="${LIVE_STAGING_HTTP_READ_DELAY_MS}" \
  -e DISCORD_VOICE_SERVICE_HTTP_READ_JITTER_MS="${LIVE_STAGING_HTTP_READ_JITTER_MS}" \
  "${service_image_ref}"

wait_for_port 55051 "discord-voice-service gRPC listener"
sleep 5

"${controller_binary}" >"${controller_log}" 2>&1

validate_success_evidence "${validation_evidence_path}"
