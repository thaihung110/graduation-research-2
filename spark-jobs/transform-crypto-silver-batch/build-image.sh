#!/bin/bash

# Build and push Docker image for Crypto OHLCV Silver Batch Job
set -e

# Configuration
IMAGE_NAME="transform-crypto-silver-batch"
REGISTRY="hungvt0110"
TAG="${1:-latest}"
FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"

echo "======================================"
echo "Building Silver Batch Transformation Job"
echo "======================================"
echo "Image: ${FULL_IMAGE}"
echo "======================================"

# Build Docker image
echo "\n🔨 Building Docker image..."
docker build -t "${FULL_IMAGE}" .

if [ $? -eq 0 ]; then
    echo "✅  Build successful!"
else
    echo "❌ Build failed!"
    exit 1
fi

# Push to registry
echo "\n📤 Pushing to Docker Hub..."
docker push "${FULL_IMAGE}"

if [ $? -eq 0 ]; then
    echo "✅ Push successful!"
    echo "\n🎉 Image ready: ${FULL_IMAGE}"
else
    echo "❌ Push failed!"
    exit 1
fi

echo "\n======================================"
echo "Build and push completed!"
echo "======================================"
