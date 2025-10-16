Purpose: Snowpark Python stored-proc entry that is callable by Admin UI to trigger assembly and evidence zippering; this stored-proc writes assembly metadata and calls external assembly service via REST. @31 @28
# assemble_document.py
from snowflake.snowpark import Session
import requests, json, uuid, datetime
def assemble_document(session: Session, request_id: str, template_id: str, clauses: dict):
    """
    - Writes an assembly run row to DOCGEN.DOCUMENT_ASSEMBLY_RUN.
    - Calls external assembly service to render PDF and upload to S3.
    - Once assembly returns, persist DOCUMENT_ARCHIVE row and call EVIDENCE_ZIPPER.
    """
    run_id = "asm_" + uuid.uuid4().hex
    session.sql(f"INSERT INTO DOCGEN.DOCUMENT_ASSEMBLY_RUN (ASSEMBLY_RUN_ID, REQUEST_ID, TEMPLATE_ID, STATUS, CREATED_AT) VALUES ('{run_id}', '{request_id}', '{template_id}', 'IN_PROGRESS', CURRENT_TIMESTAMP());").collect()
    payload = {"request_id": request_id, "template_id": template_id, "clauses": clauses}
    # Call assembly service (demo endpoint)
    r = requests.post("http://assembly-service:8000/v1/assemble", json=payload, timeout=120)
    if r.status_code != 200:
        session.sql(f"UPDATE DOCGEN.DOCUMENT_ASSEMBLY_RUN SET STATUS = 'FAILED' WHERE ASSEMBLY_RUN_ID = '{run_id}';").collect()
        return {"status": "assembly_failed", "http_status": r.status_code}
    resp = r.json()
    archive_url = resp.get("archive_url")
    # Persist DOCUMENT_ARCHIVE
    doc_id = "doc_" + uuid.uuid4().hex
    session.sql(f"INSERT INTO DOCGEN.DOCUMENT_ARCHIVE (DOCUMENT_ID, TEMPLATE_ID, ASSEMBLY_RUN_ID, DOCUMENT_URL, DOCUMENT_HASH, MIME_TYPE, SIZE_BYTES, CREATED_BY, CREATED_AT) VALUES ('{doc_id}', '{template_id}', '{run_id}', '{archive_url}', MD5('{archive_url}'), 'application/pdf', 0, 'assembly_service', CURRENT_TIMESTAMP());").collect()
    # Mark assembly run complete
    session.sql(f"UPDATE DOCGEN.DOCUMENT_ASSEMBLY_RUN SET STATUS = 'COMPLETE', COMPLETED_AT = CURRENT_TIMESTAMP() WHERE ASSEMBLY_RUN_ID = '{run_id}';").collect()
    # Call evidence zipper via SQL procedure
    session.sql(f"CALL DOCGEN.EVIDENCE_ZIPPER_SQL('{request_id}','assembly_service');").collect()
    return {"status": "assembled", "document_id": doc_id, "archive_url": archive_url}

