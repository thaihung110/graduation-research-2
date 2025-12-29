#!/bin/bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
IMAGE_NAME="finnhub-producer"
DEFAULT_TAG="latest"
REGISTRY="hungvt0110"  # Set to your registry, e.g., "docker.io/username" or "registry.example.com"
PUSH_IMAGE=false
BUILD_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --tag|-t)
            TAG="$2"
            shift 2
            ;;
        --registry|-r)
            REGISTRY="$2"
            shift 2
            ;;
        --push|-p)
            PUSH_IMAGE=true
            shift
            ;;
        --build-only|-b)
            BUILD_ONLY=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -t, --tag TAG          Docker image tag (default: latest)"
            echo "  -r, --registry REG     Docker registry URL (e.g., docker.io/username)"
            echo "  -p, --push             Push image to registry after building"
            echo "  -b, --build-only       Only build, don't push"
            echo "  -h, --help            Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Build with tag 'latest'"
            echo "  $0 -t v1.0.0                         # Build with tag 'v1.0.0'"
            echo "  $0 -r docker.io/username -p         # Build and push to registry"
            echo "  $0 -r docker.io/username -t v1.0.0 -p # Build with version and push"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Set default tag if not provided
TAG="${TAG:-$DEFAULT_TAG}"

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${SCRIPT_DIR}" || exit 1

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  FinnhubProducer Docker Build${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if Dockerfile exists
if [ ! -f "Dockerfile" ]; then
    echo -e "${RED}❌ Error: Dockerfile not found in current directory${NC}"
    exit 1
fi

# Determine full image name
if [ -n "$REGISTRY" ]; then
    FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}:${TAG}"
    LATEST_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}:latest"
else
    FULL_IMAGE_NAME="${IMAGE_NAME}:${TAG}"
    LATEST_IMAGE_NAME="${IMAGE_NAME}:latest"
fi

echo -e "${YELLOW}📦 Image Configuration:${NC}"
echo -e "   Name: ${GREEN}${FULL_IMAGE_NAME}${NC}"
if [ -n "$REGISTRY" ]; then
    echo -e "   Registry: ${GREEN}${REGISTRY}${NC}"
fi
echo -e "   Tag: ${GREEN}${TAG}${NC}"
echo ""

# Build Docker image
echo -e "${YELLOW}🔨 Building Docker image...${NC}"
if docker build -t "${FULL_IMAGE_NAME}" .; then
    echo -e "${GREEN}✅ Image built successfully: ${FULL_IMAGE_NAME}${NC}"
else
    echo -e "${RED}❌ Error: Failed to build image${NC}"
    exit 1
fi

# Tag as latest if tag is not latest
if [ "$TAG" != "latest" ]; then
    echo -e "${YELLOW}🏷️  Tagging as latest...${NC}"
    if docker tag "${FULL_IMAGE_NAME}" "${LATEST_IMAGE_NAME}"; then
        echo -e "${GREEN}✅ Tagged as latest: ${LATEST_IMAGE_NAME}${NC}"
    else
        echo -e "${YELLOW}⚠️  Warning: Failed to tag as latest${NC}"
    fi
fi

# Push to registry if requested
if [ "$PUSH_IMAGE" = true ] && [ -n "$REGISTRY" ]; then
    echo ""
    echo -e "${YELLOW}📤 Pushing image to registry...${NC}"
    
    # Check if logged in to registry
    if ! docker info | grep -q "Username"; then
        echo -e "${YELLOW}⚠️  Warning: Not logged in to Docker registry${NC}"
        echo -e "${YELLOW}   Run: docker login ${REGISTRY}${NC}"
    fi
    
    # Push main tag
    if docker push "${FULL_IMAGE_NAME}"; then
        echo -e "${GREEN}✅ Pushed: ${FULL_IMAGE_NAME}${NC}"
    else
        echo -e "${RED}❌ Error: Failed to push ${FULL_IMAGE_NAME}${NC}"
        exit 1
    fi
    
    # Push latest tag if different
    if [ "$TAG" != "latest" ]; then
        if docker push "${LATEST_IMAGE_NAME}"; then
            echo -e "${GREEN}✅ Pushed: ${LATEST_IMAGE_NAME}${NC}"
        else
            echo -e "${YELLOW}⚠️  Warning: Failed to push latest tag${NC}"
        fi
    fi
    
    echo ""
    echo -e "${GREEN}✅ Image pushed successfully!${NC}"
elif [ "$PUSH_IMAGE" = true ] && [ -z "$REGISTRY" ]; then
    echo -e "${YELLOW}⚠️  Warning: --push specified but no registry provided${NC}"
    echo -e "${YELLOW}   Use --registry to specify registry URL${NC}"
elif [ "$BUILD_ONLY" = true ]; then
    echo ""
    echo -e "${GREEN}✅ Build completed (push skipped)${NC}"
fi

# Show image info
echo ""
echo -e "${BLUE}📊 Image Information:${NC}"
docker images | grep -E "^(${REGISTRY:+${REGISTRY}/}${IMAGE_NAME}|${IMAGE_NAME})" | head -5

echo ""
echo -e "${GREEN}✅ Done!${NC}"
echo ""
echo -e "${YELLOW}💡 Tips:${NC}"
echo -e "   To run locally: ${BLUE}docker run ${FULL_IMAGE_NAME}${NC}"
if [ -n "$REGISTRY" ]; then
    echo -e "   To pull: ${BLUE}docker pull ${FULL_IMAGE_NAME}${NC}"
fi
if [ "$PUSH_IMAGE" = false ] && [ -n "$REGISTRY" ]; then
    echo -e "   To push: ${BLUE}$0 -r ${REGISTRY} -t ${TAG} -p${NC}"
fi

