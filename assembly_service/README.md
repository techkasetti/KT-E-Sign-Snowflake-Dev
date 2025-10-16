Assembly service: run with env vars set (EVIDENCE_BUCKET, Snowflake creds). This service uploads assembled documents to S3 and calls DOCGEN.WRITE_EVIDENCE_BUNDLE to persist metadata and evidence manifest, following the evidence uploader patterns in your design docs @186 @65.

