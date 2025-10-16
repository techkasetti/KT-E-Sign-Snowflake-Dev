# reconcile_evidence_exports.py
# Snowpark Python stored-proc to reconcile an evidence export manifest with S3 contents and update EVIDENCE_RECONCILIATION.
from snowflake.snowpark import Session
import json, subprocess, uuid, datetime
def reconcile_evidence_exports(session: Session, manifest_id: str):
    """Reconcile the manifest row MANIFEST_ID by listing S3 prefix and comparing counts, then write DOCGEN.EVIDENCE_RECONCILIATION."""
    # Fetch manifest row
    rec = session.sql(f"SELECT MANIFEST_ID, S3_PATH, ROW_COUNT FROM DOCGEN.EVIDENCE_EXPORT_MANIFEST WHERE MANIFEST_ID = '{manifest_id}'").collect()
    if not rec:
        return {"error":"manifest_not_found"}
    s3_path = rec[0]['S3_PATH']
    expected = int(rec[0]['ROW_COUNT'])
    # List S3 key count (demo uses awscli list; in prod use API integration)
    cmd = ["aws","s3","ls", s3_path]
    try:
        out = subprocess.check_output(cmd).decode('utf-8').strip()
        # If object listed, assume uploaded (for single-file manifest)
        actual = 1 if out else 0
    except Exception:
        actual = 0
    status = 'OK' if actual == expected else 'MISMATCH'
    recon_id = "recon_" + uuid.uuid4().hex
    session.sql(f"""
        INSERT INTO DOCGEN.EVIDENCE_RECONCILIATION (RECON_ID, MANIFEST_ID, EXPECTED_ROWS, ACTUAL_ROWS, STATUS, CHECKED_AT)
        VALUES ('{recon_id}', '{manifest_id}', {expected}, {actual}, '{status}', CURRENT_TIMESTAMP());
    """).collect()
    return {"recon_id": recon_id, "status": status, "expected": expected, "actual": actual}

