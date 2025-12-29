#!/bin/bash

set -eu

APP_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${APP_DIR}" || exit 1

echo "Installing FinnhubProducer..."
kubectl apply -f application/finnhub-producer.yaml

echo "Waiting for deployment to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/finnhub-producer -n default || true

echo "Checking deployment status..."
kubectl get deployment finnhub-producer -n default

echo "Checking pods..."
kubectl get pods -n default -l app=finnhub-producer

echo ""
echo "FinnhubProducer installed successfully!"
echo ""
echo "To view logs:"
echo "  kubectl logs -f deployment/finnhub-producer -n default"
echo ""
echo "To check status:"
echo "  kubectl get pods -n default -l app=finnhub-producer"

