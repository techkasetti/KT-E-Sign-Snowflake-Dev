Purpose: Validate signature integrity (hash checks), persist validation record, optionally call HSM sign verifier via External Function.
# validate_signature_and_record.py
from snowflake.snowpark import Session
import uuid, json

def validate_signature_and_record(session: Session, request_id: str, signer_id: str, signature_b64: str, cert_chain_variant=None):
    """
    Basic validation: compute a verification hash (demo) and insert validation record.
    In production, call HSM or verifier External Function prior to writing 'SIGNED_BY_HSM'.
    """
    # Simple demo validation hash (MD5 of base64 payload)
    res = session.sql(f"SELECT MD5('{signature_b64}') AS sig_hash").collect()
    sig_hash = res[0]['SIG_HASH'] if res else None
    validation_id = "val_" + uuid.uuid4().hex
    session.sql(f"""
        INSERT INTO DOCGEN.SIGNATURE_VALIDATION (VALIDATION_ID, REQUEST_ID, SIGNER_ID, VALIDATION_STATUS, VALIDATION_HASH, SIGNATURE_BLOB_BASE64, CERT_CHAIN, VERIFIED_AT)
        VALUES ('{validation_id}', '{request_id}', '{signer_id}', 'SIGNED', '{sig_hash}', '{signature_b64}', PARSE_JSON('{json.dumps(cert_chain_variant or {})}'), CURRENT_TIMESTAMP());
    """).collect()
    return {"validation_id": validation_id, "validation_hash": sig_hash}

