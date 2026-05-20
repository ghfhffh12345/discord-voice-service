#!/usr/bin/env bash
set -euo pipefail

required_vars=(
  APPLICATION_ID
  BOT_TOKEN
  TEST_GUILD_ID
  TEST_VOICE_CHANNEL_ID
  TEST_VIDEO_ID
  DISCORD_VOICE_SERVICE_URI
  DISCORD_VOICE_SERVICE_BIND_ADDR
  DISCORD_VOICE_SERVICE_IMAGE_REF
  DISCORD_VOICE_SERVICE_YTMUSIC_ADDR
)

required_tools=(
  podman
  cargo
  rustc
  skopeo
)

for tool in "${required_tools[@]}"; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "::error::${tool} must already be installed on the self-hosted staging runner"
    exit 1
  fi
done

for required_var in "${required_vars[@]}"; do
  if [[ -z "${!required_var:-}" ]]; then
    echo "::error::${required_var} is required for live staging validation"
    exit 1
  fi
done

browser_json_source_path="${BROWSER_JSON_SOURCE_PATH:-${STAGING_BROWSER_JSON_SOURCE_PATH:-}}"
if [[ -z "${browser_json_source_path}" ]]; then
  echo "::error::Set BROWSER_JSON_SOURCE_PATH or STAGING_BROWSER_JSON_SOURCE_PATH to a browser.json path outside the workspace"
  exit 1
fi

if [[ ! -f "${browser_json_source_path}" ]]; then
  echo "::error::browser.json source path does not exist: ${browser_json_source_path}"
  exit 1
fi

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

podman --version
cargo --version
rustc --version
skopeo --version
