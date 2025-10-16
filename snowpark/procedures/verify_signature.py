# verify_signature.py
from snowflake.snowpark import Session
import hashlib, json

def verify_signature(session: Session, request_id: str, signer_id: str):
    # Retrieve document hash from DOCUMENT_ARCHIVE
    df = session.sql(f"""
        SELECT DOCUMENT_HASH
        FROM DOCGEN.DOCUMENT_ARCHIVE
        WHERE DOCUMENT_ID = (
            SELECT DOCUMENT_ID FROM DOCGEN.SIGNATURE_REQUESTS WHERE REQUEST_ID = '{request_id}' LIMIT 1
        )
    """).collect()

    if not df:
        return {"status": "error", "reason": "document not found"}

    doc_hash = df[0]['DOCUMENT_HASH']
    validation_hash = hashlib.sha256((request_id + signer_id + doc_hash).encode()).hexdigest()

    session.sql(f"""
        INSERT INTO DOCGEN.SIGNATURE_VALIDATION (VALIDATION_ID, REQUEST_ID, SIGNER_ID, VALIDATION_STATUS, VALIDATION_HASH, VERIFIED_AT)
        VALUES ('val_{request_id}_{signer_id}','{request_id}','{signer_id}','VERIFIED','{validation_hash}',CURRENT_TIMESTAMP())
    """).collect()

    # Optionally assemble evidence bundle
    session.sql(f"CALL DOCGEN.WRITE_EVIDENCE_BUNDLE(PARSE_JSON('{{\"request_id\":\"{request_id}\",\"signer_id\":\"{signer_id}\"}}'))").collect()

    return {"status": "verified", "validation_hash": validation_hash}

