#!/bin/bash
# =============================================================================
# build-image.sh — Build and push the streaming-test Spark Docker image
#
# Usage:
#   ./build-image.sh              # build & push with tag 'latest'
#   ./build-image.sh v1.0         # build & push with tag 'v1.0'
#   PUSH_TO_DOCKERHUB=false ./build-image.sh   # local build only
#
# Environment overrides:
#   DOCKERHUB_USERNAME  — Docker Hub username  (default: hungvt0110)
#   IMAGE_NAME          — image name            (default: streaming-test)
#   IMAGE_TAG           — image tag             (default: latest or $1)
#   PUSH_TO_DOCKERHUB   — push after build?     (default: true)
# =============================================================================

set -eu

# ── Configuration ────────────────────────────────────────────────────────────
DOCKERHUB_USERNAME="${DOCKERHUB_USERNAME:-hungvt0110}"
IMAGE_NAME="${IMAGE_NAME:-streaming-test}"
IMAGE_TAG="${1:-${IMAGE_TAG:-latest}}"
PUSH_TO_DOCKERHUB="${PUSH_TO_DOCKERHUB:-true}"

FULL_IMAGE_NAME="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${IMAGE_TAG}"
LATEST_TAG="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:latest"

# Change to the directory containing this script (i.e. the job root)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${SCRIPT_DIR}"

# ── Banner ───────────────────────────────────────────────────────────────────
echo "=============================================="
echo " Build: streaming-test Spark Docker Image"
echo "=============================================="
echo " Docker Hub User : ${DOCKERHUB_USERNAME}"
echo " Image Name      : ${IMAGE_NAME}"
echo " Tag             : ${IMAGE_TAG}"
echo " Full Image      : ${FULL_IMAGE_NAME}"
echo " Push to Hub     : ${PUSH_TO_DOCKERHUB}"
echo "=============================================="

# ── Step 1: Build ────────────────────────────────────────────────────────────
echo ""
echo "Step 1: Building Docker image..."
docker build -t "${FULL_IMAGE_NAME}" .

# Also tag as :latest when a version tag is specified
if [ "${IMAGE_TAG}" != "latest" ]; then
    echo "Tagging as latest: ${LATEST_TAG}"
    docker tag "${FULL_IMAGE_NAME}" "${LATEST_TAG}"
fi

echo ""
echo "Build complete: ${FULL_IMAGE_NAME}"

# ── Step 2: Push ─────────────────────────────────────────────────────────────
if [ "${PUSH_TO_DOCKERHUB}" = "true" ]; then
    echo ""
    echo "Step 2: Pushing to Docker Hub..."
    echo "  (Make sure you are logged in:  docker login -u ${DOCKERHUB_USERNAME})"
    echo ""

    docker push "${FULL_IMAGE_NAME}"

    if [ "${IMAGE_TAG}" != "latest" ]; then
        docker push "${LATEST_TAG}"
    fi

    echo ""
    echo "=============================================="
    echo " Image pushed successfully!"
    echo " ${FULL_IMAGE_NAME}"
    echo "=============================================="
else
    echo ""
    echo "=============================================="
    echo " Image built locally (not pushed)."
    echo " To push: PUSH_TO_DOCKERHUB=true ./build-image.sh ${IMAGE_TAG}"
    echo "=============================================="
fi
