#!/bin/bash

# Script to start/deploy the transform-crypto-silver SparkApplication
# This job transforms crypto trades from Bronze to Silver layer

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JOBS_DIR="$( cd "${SCRIPT_DIR}/../applications/spark/silver-layer/jobs" && pwd )"
NAMESPACE="default"
JOB_NAME="transform-crypto-silver-batch"
MANIFEST_FILE="${JOBS_DIR}/${JOB_NAME}.yaml"

echo "=========================================="
echo "Starting Transform Crypto Silver Spark Job"
echo "=========================================="
echo "Job Name: ${JOB_NAME}"
echo "Namespace: ${NAMESPACE}"
echo "Manifest: ${MANIFEST_FILE}"
echo "=========================================="
echo ""

# Check if manifest exists
if [ ! -f "${MANIFEST_FILE}" ]; then
    echo "❌ Error: Manifest file not found: ${MANIFEST_FILE}"
    exit 1
fi

# Check if job already exists
if kubectl get sparkapplication "${JOB_NAME}" -n "${NAMESPACE}" &>/dev/null; then
    echo "⚠️  SparkApplication '${JOB_NAME}' already exists"
    echo ""
    echo "Current status:"
    kubectl get sparkapplication "${JOB_NAME}" -n "${NAMESPACE}"
    echo ""
    read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Deleting existing SparkApplication..."
        kubectl delete sparkapplication "${JOB_NAME}" -n "${NAMESPACE}" --wait=true
        echo "✅ Deleted"
        echo ""
    else
        echo "❌ Aborted. Use 'stop_transform_crypto_silver.sh' to stop the job first."
        exit 1
    fi
fi

# Apply manifest
echo "📋 Applying SparkApplication manifest..."
kubectl apply -f "${MANIFEST_FILE}"

# Wait a moment for resource to be created
sleep 2

# Check status
echo ""
echo "⏳ Waiting for SparkApplication to be created..."
kubectl wait --for=condition=ready sparkapplication "${JOB_NAME}" -n "${NAMESPACE}" --timeout=30s || true

echo ""
echo "📊 SparkApplication status:"
kubectl get sparkapplication "${JOB_NAME}" -n "${NAMESPACE}"

echo ""
echo "=========================================="
echo "✅ SparkApplication deployed successfully!"
echo "=========================================="
echo ""
echo "📋 Useful commands:"
echo "   # View status:"
echo "   kubectl get sparkapplication ${JOB_NAME} -n ${NAMESPACE}"
echo ""
echo "   # View driver logs:"
echo "   kubectl logs -l spark-role=driver,spark-app-name=${JOB_NAME} -n ${NAMESPACE} -f"
echo ""
echo "   # View executor logs:"
echo "   kubectl logs -l spark-role=executor,spark-app-name=${JOB_NAME} -n ${NAMESPACE} -f"
echo ""
echo "   # Describe for details:"
echo "   kubectl describe sparkapplication ${JOB_NAME} -n ${NAMESPACE}"
echo ""
echo "   # Stop the job:"
echo "   ${SCRIPT_DIR}/stop_transform_crypto_silver.sh"
echo "=========================================="

