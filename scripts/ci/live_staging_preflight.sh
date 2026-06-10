#!/usr/bin/env bash
set -euo pipefail

required_vars=(
  APPLICATION_ID
  BOT_TOKEN
  OBSERVER_BOT_TOKEN
  TEST_GUILD_ID
  TEST_VOICE_CHANNEL_ID
  TEST_VIDEO_ID
  BROWSER_JSON
  DISCORD_VOICE_SERVICE_URI
  DISCORD_VOICE_SERVICE_BIND_ADDR
  DISCORD_VOICE_SERVICE_IMAGE_REF
  DISCORD_VOICE_SERVICE_YTMUSIC_ADDR
  LIVE_STAGING_PROFILE
  LIVE_STAGING_SERVICE_CPUS
  LIVE_STAGING_CPU_CONTENTION_WORKERS
  LIVE_STAGING_HTTP_READ_DELAY_MS
  LIVE_STAGING_HTTP_READ_JITTER_MS
)

required_tools=(
  docker
  cargo
  rustc
  python3
  skopeo
)

for tool in "${required_tools[@]}"; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "::error::${tool} must be available on the GitHub-hosted live-staging runner"
    exit 1
  fi
done

for required_var in "${required_vars[@]}"; do
  if [[ -z "${!required_var:-}" ]]; then
    echo "::error::${required_var} is required for live staging validation"
    exit 1
  fi
done

for numeric_var in \
  LIVE_STAGING_CPU_CONTENTION_WORKERS \
  LIVE_STAGING_HTTP_READ_DELAY_MS \
  LIVE_STAGING_HTTP_READ_JITTER_MS; do
  if [[ ! "${!numeric_var}" =~ ^[0-9]+$ || "${!numeric_var}" -eq 0 ]]; then
    echo "::error::${numeric_var} must be a positive integer"
    exit 1
  fi
done

resolved_digest="$(
  skopeo inspect --format '{{.Digest}}' "docker://${DISCORD_VOICE_SERVICE_IMAGE_REF}"
)"
resolved_image_ref="${DISCORD_VOICE_SERVICE_IMAGE_REF%@*}@${resolved_digest}"

if [[ -n "${DISCORD_VOICE_SERVICE_IMAGE_DIGEST:-}" ]]; then
  if [[ "${resolved_digest}" != "${DISCORD_VOICE_SERVICE_IMAGE_DIGEST}" ]]; then
    echo "::error::discord-voice-service image digest mismatch for ${DISCORD_VOICE_SERVICE_IMAGE_REF}: expected ${DISCORD_VOICE_SERVICE_IMAGE_DIGEST}, got ${resolved_digest}"
    exit 1
  fi
fi

export DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF="${resolved_image_ref}"
if [[ -n "${GITHUB_ENV:-}" ]]; then
  {
    echo "DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_REF=${resolved_image_ref}"
    echo "DISCORD_VOICE_SERVICE_RESOLVED_IMAGE_DIGEST=${resolved_digest}"
  } >> "${GITHUB_ENV}"
fi

docker --version
cargo --version
rustc --version
skopeo --version
