# export_evidence_to_s3.py
# Snowpark Python stored proc (dev/demo) that selects evidence bundle rows and writes a JSONL file to local /tmp then uploads to demo S3 using boto3.
# In production prefer containerized Snowpark with proper IAM roles or use COPY INTO @stage -> put to S3 via STORAGE_INTEGRATION pattern.

import os
import json
import uuid
import boto3
from snowflake.snowpark import Session

def export_evidence_to_s3(session: Session, bucket_prefix: str = "demo-export", days_back: int = 90):
    """
    Export EVIDENCE_BUNDLE rows older than days_back to S3 under bucket_prefix.
    This is a dev/demo implementation using boto3 and hardcoded demo credentials.
    """
    # Query evidence bundles older than days_back
    sql = f"""
    SELECT BUNDLE_ID, DOCUMENT_ID, ACCOUNT_ID, BUNDLE_HASH, METADATA, CREATED_AT
    FROM DOCGEN.EVIDENCE_BUNDLE
    WHERE CREATED_AT <= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
    ORDER BY CREATED_AT ASC
    """
    rows = session.sql(sql).collect()
    if not rows:
        return {"status": "no_rows"}
    # Prepare JSONL file
    file_name = f"/tmp/evidence_export_{uuid.uuid4().hex}.jsonl"
    with open(file_name, "w", encoding="utf-8") as fh:
        for r in rows:
            rec = {
                "bundle_id": r["BUNDLE_ID"],
                "document_id": r["DOCUMENT_ID"],
                "account_id": r["ACCOUNT_ID"],
                "bundle_hash": r["BUNDLE_HASH"],
                "metadata": r["METADATA"],
                "created_at": str(r["CREATED_AT"])
            }
            fh.write(json.dumps(rec) + "\n")
    # Upload to demo S3 (demo credentials for dev)
    s3 = boto3.client(
        "s3",
        aws_access_key_id="DEMO_AWS_ACCESS_KEY",
        aws_secret_access_key="DEMO_AWS_SECRET_KEY",
        region_name="us-east-1"
    )
    dest_key = f"{bucket_prefix}/evidence_export_{uuid.uuid4().hex}.jsonl"
    s3.upload_file(file_name, "demo-bucket-12345", dest_key)
    # Record export manifest row in a simple manifest table (create if not exists)
    session.sql(f"""
    CREATE TABLE IF NOT EXISTS DOCGEN.EVIDENCE_EXPORT_MANIFEST (
      MANIFEST_ID STRING PRIMARY KEY,
      S3_PATH STRING,
      ROW_COUNT NUMBER,
      EXPORT_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
    );
    """).collect()
    manifest_id = "manifest_" + uuid.uuid4().hex
    session.sql(f"""
      INSERT INTO DOCGEN.EVIDENCE_EXPORT_MANIFEST (MANIFEST_ID, S3_PATH, ROW_COUNT)
      VALUES ('{manifest_id}', 's3://demo-bucket-12345/{dest_key}', {len(rows)});
    """).collect()
    return {"status": "exported", "manifest_id": manifest_id, "s3_path": f"s3://demo-bucket-12345/{dest_key}", "rows": len(rows)}

