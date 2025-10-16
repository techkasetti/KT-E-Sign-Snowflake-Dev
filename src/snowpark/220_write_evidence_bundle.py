# WRITE_EVIDENCE_BUNDLE Snowpark procedure: assembles CompliancePacket metadata row and writes archive manifest to stage/S3 per evidence patterns. @28 @31
from snowflake.snowpark import Session
import uuid, json
def handler(session: Session, request_id: str, archive_path: str):
    bundle_id = str(uuid.uuid4())
    manifest = {"bundle_id": bundle_id, "request_id": request_id, "archive_path": archive_path}
    session.sql(f"""
      INSERT INTO DOCGEN.SIGNATURE_EVIDENCE_BUNDLE(BUNDLE_ID, REQUEST_ID, BUNDLE_URL, BUNDLE_MANIFEST)
      VALUES ('{bundle_id}','{request_id}','{archive_path}', PARSE_JSON('{json.dumps(manifest)}'))
    """).collect()
    return {"bundle_id": bundle_id}

