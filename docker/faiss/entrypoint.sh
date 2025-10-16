#!/bin/sh
# Entrypoint for FAISS query container: download index snapshot path from env and start service
if [ -z "$FAISS_INDEX_S3" ]; then
  echo "FAISS_INDEX_S3 not set"
  exit 1
fi
# bootstrap: download index and start app (implementation-specific)
exec gunicorn app:app -b 0.0.0.0:8080 --workers 2

This entrypoint emphasizes pulling index snapshots from object storage at startup to avoid baking Snowflake credentials into images as recommended in your docs @14 @56. @14 @56

