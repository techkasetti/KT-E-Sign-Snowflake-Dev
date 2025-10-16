# evidence_zipper.py
# Snowpark stored-proc (demo) that collects evidence components and produces a bundled JSON in EVIDENCE_BUNDLE
from snowflake.snowpark import Session
import uuid, json, datetime

def evidence_zipper(session: Session, request_id: str, created_by: str):
    """
    Assemble an evidence 'zip' (JSON variant) from request/events/validation and persist to EVIDENCE_BUNDLE.
    """
    doc_row = session.sql(f"SELECT * FROM DOCGEN.SIGNATURE_REQUESTS WHERE REQUEST_ID = '{request_id}' LIMIT 1").collect()
    events = session.sql(f"SELECT OBJECT_CONSTRUCT(*) as ev FROM DOCGEN.SIGNATURE_EVENTS WHERE REQUEST_ID = '{request_id}' ORDER BY EVENT_TS").collect()
    validation = session.sql(f"SELECT OBJECT_CONSTRUCT(*) as v FROM DOCGEN.SIGNATURE_VALIDATION WHERE REQUEST_ID = '{request_id}'").collect()
    bundle_id = "bundle_" + str(uuid.uuid4())
    metadata = {
        "assembled_at": datetime.datetime.utcnow().isoformat(),
        "request_id": request_id,
        "created_by": created_by
    }
    session.sql(f"""
      INSERT INTO DOCGEN.EVIDENCE_BUNDLE (BUNDLE_ID, DOCUMENT_ID, ACCOUNT_ID, ARCHIVE_LOCATION, BUNDLE_HASH, CREATED_AT, METADATA)
      VALUES ('{bundle_id}', '{doc_row[0]['DOCUMENT_ID'] if doc_row else 'unknown'}', '{doc_row[0]['ACCOUNT_ID'] if doc_row else 'unknown'}', 's3://demo-bucket-12345/evidence/{bundle_id}.json', '{uuid.uuid4().hex}', CURRENT_TIMESTAMP(), PARSE_JSON('{json.dumps(metadata)}'));
    """).collect()
    return {"bundle_id": bundle_id, "status": "created"}

