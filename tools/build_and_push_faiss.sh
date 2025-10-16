#!/usr/bin/env bash
# Build and push FAISS query service container image (concrete image repo used here) @63
set -e
IMAGE="registry.prod.example.com/faiss-docgen:latest"
docker build -t $IMAGE services/faiss_query_service
docker push $IMAGE
echo "Image pushed: $IMAGE"

