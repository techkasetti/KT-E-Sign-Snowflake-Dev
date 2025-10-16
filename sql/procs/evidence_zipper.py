Purpose: Assemble a CompliancePacket (evidence bundle) for a completed signature request, store metadata in DOCGEN.EVIDENCE_BUNDLE, and return the bundle_id and archive URL. @70 @113
# evidence_zipper.py
from snowflake.snowpark import Session
import json, uuid, datetime, subprocess, os
def evidence_zipper(session: Session, request_id: str, created_by: str):
    """
    - Collects document metadata, signature events, validation rows and any attached files.
    - Produces a JSON manifest and writes a row to DOCGEN.EVIDENCE_BUNDLE.
    - Returns: {bundle_id, archive_location, bundle_hash}
    """
    # Build manifest rows
    doc = session.sql(f"SELECT DOCUMENT_ID, ACCOUNT_ID, DOCUMENT_URL, DOCUMENT_HASH FROM DOCGEN.SIGNATURE_REQUESTS sr JOIN DOCGEN.DOCUMENT_ARCHIVE da ON sr.DOCUMENT_ID = da.DOCUMENT_ID WHERE sr.REQUEST_ID = '{request_id}' LIMIT 1").collect()
    if not doc:
        return {"error": "request_not_found"}
    document = doc[0]
    events = session.sql(f"SELECT EVENT_ID, EVENT_TYPE, EVENT_TS, SIGNER_ID, IP_ADDR, USER_AGENT FROM DOCGEN.SIGNATURE_EVENTS WHERE REQUEST_ID = '{request_id}' ORDER BY EVENT_TS").collect()
    validations = session.sql(f"SELECT VALIDATION_ID, VALIDATION_STATUS, VALIDATION_HASH, VERIFIED_AT FROM DOCGEN.SIGNATURE_VALIDATION WHERE REQUEST_ID = '{request_id}'").collect()
    manifest = {
        "request_id": request_id,
        "document": {
            "document_id": document['DOCUMENT_ID'],
            "document_url": document['DOCUMENT_URL'],
            "document_hash": document['DOCUMENT_HASH']
        },
        "events": [ { "event_id": r['EVENT_ID'], "type": r['EVENT_TYPE'], "ts": str(r['EVENT_TS']), "signer_id": r['SIGNER_ID'], "ip": r['IP_ADDR'], "ua": r['USER_AGENT'] } for r in events ],
        "validations": [ { "validation_id": v['VALIDATION_ID'], "status": v['VALIDATION_STATUS'], "hash": v['VALIDATION_HASH'], "verified_at": str(v['VERIFIED_AT']) } for v in validations ],
        "assembled_at": datetime.datetime.utcnow().isoformat(),
        "created_by": created_by
    }
    # Write manifest to local file and upload via awscli to S3 manifest area (demo pattern)
    bundle_id = "bundle_" + uuid.uuid4().hex
    local_path = f"/tmp/{bundle_id}.json"
    with open(local_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(manifest, ensure_ascii=False))
    s3_path = f"s3://docgen-evidence-archive/{bundle_id}.json"
    subprocess.check_call(["aws", "s3", "cp", local_path, s3_path])
    bundle_hash = session.sql(f"SELECT MD5('{json.dumps(manifest)}') AS h").collect()[0]['H']
    # Persist evidence bundle metadata
    session.sql(f"""
        INSERT INTO DOCGEN.EVIDENCE_BUNDLE (BUNDLE_ID, DOCUMENT_ID, ACCOUNT_ID, ARCHIVE_LOCATION, BUNDLE_HASH, METADATA, CREATED_AT)
        VALUES ('{bundle_id}', '{document['DOCUMENT_ID']}', '{document['ACCOUNT_ID']}', '{s3_path}', '{bundle_hash}', PARSE_JSON('{json.dumps(manifest)}'), CURRENT_TIMESTAMP());
    """).collect()
    try:
        os.remove(local_path)
    except Exception:
        pass
    return {"bundle_id": bundle_id, "archive_location": s3_path, "bundle_hash": bundle_hash}

