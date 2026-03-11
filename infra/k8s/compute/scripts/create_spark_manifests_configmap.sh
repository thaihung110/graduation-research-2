#!/bin/bash

# Script to create ConfigMap containing Spark job manifests for Airflow DAGs
# This ConfigMap will be mounted into Airflow pods to access SparkApplication YAML files

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BRONZE_MANIFEST_DIR="$( cd "${SCRIPT_DIR}/../applications/spark/legacy" && pwd )"
SILVER_MANIFEST_DIR="$( cd "${SCRIPT_DIR}/../applications/spark/silver-layer/jobs" && pwd )"
NAMESPACE="default"

echo "=========================================="
echo "Creating Spark Manifests ConfigMap"
echo "=========================================="
echo "Namespace: ${NAMESPACE}"
echo "Bronze Manifest Dir: ${BRONZE_MANIFEST_DIR}"
echo "Silver Manifest Dir: ${SILVER_MANIFEST_DIR}"
echo "=========================================="
echo ""

# Check if manifest files exist
if [ ! -f "${BRONZE_MANIFEST_DIR}/taxi-data-ingestion.yaml" ]; then
    echo "❌ Manifest not found: taxi-data-ingestion.yaml"
    exit 1
fi

if [ ! -f "${SILVER_MANIFEST_DIR}/transform-crypto-silver-batch.yaml" ]; then
    echo "❌ Manifest not found: transform-crypto-silver-batch.yaml"
    exit 1
fi

# Create ConfigMap from manifest files
echo "📦 Creating/updating ConfigMap 'spark-manifests'..."
kubectl create configmap spark-manifests \
    --from-file=taxi-data-ingestion.yaml="${BRONZE_MANIFEST_DIR}/taxi-data-ingestion.yaml" \
    --from-file=transform-crypto-silver-batch.yaml="${SILVER_MANIFEST_DIR}/transform-crypto-silver-batch.yaml" \
    -n "${NAMESPACE}" \
    --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "✅ ConfigMap 'spark-manifests' created/updated successfully!"
echo ""
echo "📋 Manifest files in ConfigMap:"
kubectl get configmap spark-manifests -n ${NAMESPACE} -o jsonpath='{.data}' | jq 'keys'

echo ""
echo "📋 Verify ConfigMap:"
echo "   kubectl get configmap spark-manifests -n ${NAMESPACE}"
echo ""
echo "📋 View specific manifest:"
echo "   kubectl get configmap spark-manifests -n ${NAMESPACE} -o jsonpath='{.data.transform-crypto-silver-batch\.yaml}'"
echo "=========================================="
