#!/usr/bin/env bash
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/ddl/maintenance_stored_procs.sql
echo "Maintenance SQL deployed."

-- End of tranche: additional FAISS snapshot manifest, FAISS query service and Dockerfile, External Function registration, index-registration stored-proc, retrieval wrapper, reconciliation procedures, UDFs, maintenance procs, CI jobs and tests added as full concrete artifacts following the Snowpark/External Function and FAISS snapshot patterns in your documents. @63 @314 @296

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.Output with maximum output

- Anara: FILENAME:sql/procs/export_evidence_to_s3.py
# export_evidence_to_s3.py
# Snowpark Python stored-proc that exports evidence bundle rows to a JSONL manifest file and uploads to S3,
# then records an entry in EVIDENCE_EXPORT_MANIFEST for reconciliation.
from snowflake.snowpark import Session
import json, uuid, datetime, subprocess, os

def export_evidence_to_s3(session: Session, manifest_id: str, s3_prefix: str, cut_off_days: int = 365):
    """
    Export EVIDENCE_BUNDLE rows older than cut_off_days to a local JSONL, upload to S3 prefix, and record manifest.
    Returns manifest_id, s3_path, row_count.
    """
    # Determine cutoff
    cutoff_ts = session.sql(f"SELECT DATEADD('day', -{int(cut_off_days)}, CURRENT_TIMESTAMP()) AS cutoff").collect()[0]['CUTOFF']
    # Query bundles to export
    rows = session.sql(f"SELECT BUNDLE_ID, DOCUMENT_ID, ACCOUNT_ID, ARCHIVE_LOCATION, BUNDLE_HASH, METADATA, CREATED_AT FROM DOCGEN.EVIDENCE_BUNDLE WHERE CREATED_AT <= TO_TIMESTAMP_LTZ('{cutoff_ts}')").collect()
    # Write local file
    local_fname = f"/tmp/{manifest_id}.jsonl"
    with open(local_fname, "w", encoding="utf-8") as fh:
        for r in rows:
            rec = {
                "bundle_id": r['BUNDLE_ID'],
                "document_id": r['DOCUMENT_ID'],
                "account_id": r['ACCOUNT_ID'],
                "archive_location": r['ARCHIVE_LOCATION'],
                "bundle_hash": r['BUNDLE_HASH'],
                "metadata": r['METADATA'],
                "created_at": r['CREATED_AT'].isoformat() if r['CREATED_AT'] else None
            }
            fh.write(json.dumps(rec) + "\n")
    # Upload to S3 using awscli (concrete demo credentials must be provisioned in environment)
    s3_path = f"{s3_prefix.rstrip('/')}/{manifest_id}.jsonl"
    subprocess.check_call(["aws", "s3", "cp", local_fname, s3_path])
    row_count = len(rows)
    # Insert manifest record
    session.sql(f"""
        INSERT INTO DOCGEN.EVIDENCE_EXPORT_MANIFEST (MANIFEST_ID, S3_PATH, ROW_COUNT, EXPORT_TS)
        VALUES ('{manifest_id}', '{s3_path}', {row_count}, CURRENT_TIMESTAMP());
    """).collect()
    # Cleanup local file
    try:
        os.remove(local_fname)
    except Exception:
        pass
    return {"manifest_id": manifest_id, "s3_path": s3_path, "row_count": row_count}

This stored-proc exports EVIDENCE_BUNDLE rows older than the configured cutoff to an S3-hosted JSONL manifest and records the manifest for later reconciliation @67 @68.

