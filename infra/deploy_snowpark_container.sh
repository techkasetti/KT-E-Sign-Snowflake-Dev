#!/usr/bin/env bash
# Build & push FAISS agent container; update IMAGE var to your registry
set -e
IMAGE=${IMAGE:-"your-registry/ai-feature-agent:latest"}
docker build -t ${IMAGE} ./faiss
docker push ${IMAGE}
echo "Container pushed: ${IMAGE}"
echo "Register container with Snowflake via Snowpark container registration if used; follow RUNBOOK."

