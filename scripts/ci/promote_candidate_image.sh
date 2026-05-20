#!/usr/bin/env bash
set -euo pipefail

: "${SOURCE_IMAGE_REPO:?SOURCE_IMAGE_REPO is required}"
: "${SOURCE_IMAGE_DIGEST:?SOURCE_IMAGE_DIGEST is required}"
: "${TARGET_TAGS:?TARGET_TAGS is required}"

for tag in ${TARGET_TAGS}; do
  echo "Promoting docker://${SOURCE_IMAGE_REPO}@${SOURCE_IMAGE_DIGEST} to docker://${SOURCE_IMAGE_REPO}:${tag}"
  skopeo copy --all \
    "docker://${SOURCE_IMAGE_REPO}@${SOURCE_IMAGE_DIGEST}" \
    "docker://${SOURCE_IMAGE_REPO}:${tag}"
done
