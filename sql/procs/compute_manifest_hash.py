# compute_manifest_hash.py
from snowflake.snowpark import Session
import hashlib, json
def compute_manifest_hash(session: Session, manifest_id: str):
    rows = session.sql(f"SELECT S3_PATH, ROW_COUNT FROM DOCGEN.EVIDENCE_EXPORT_MANIFEST WHERE MANIFEST_ID = '{manifest_id}'").collect()
    if not rows:
        return {"error":"manifest not found"}
    payload = json.dumps({"s3_path": rows[0]['S3_PATH'], "row_count": rows[0]['ROW_COUNT']}, sort_keys=True)
    h = hashlib.sha256(payload.encode()).hexdigest()
    session.sql(f"INSERT INTO DOCGEN.EVIDENCE_RECONCILIATION (RECON_ID, MANIFEST_ID, STATUS, DETAILS, CREATED_AT) VALUES ('recon_'||RANDOM(), '{manifest_id}', 'HASHED', PARSE_JSON('{{\"hash\":\"{h}\"}}'), CURRENT_TIMESTAMP());").collect()
    return {"manifest_id": manifest_id, "hash": h}

Manifest hashing supports deterministic reconciliation and auditability as described in your evidence/export/runbook materials @23 @31. @23 @31

