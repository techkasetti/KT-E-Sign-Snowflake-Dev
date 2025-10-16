Purpose: Dev-mode HSM signer stub stored-proc that simulates signing, stores signature blobs in SIGNATURE_VALIDATION and returns signature hash; intended to be replaced by HSM-backed production integration. @96 @70
# hsm_signer_stub.py
from snowflake.snowpark import Session
import uuid, hashlib, datetime
def hsm_signer_stub(session: Session, request_id: str, signer_id: str):
    """
    - Simulates HSM signing; writes SIGNATURE_VALIDATION row with status SIGNED_BY_HSM and returns validation_id.
    - Production note: replace with an HSM-backed signer that returns a real signature blob and cert chain.
    """
    signature = f"{request_id}:{signer_id}:{uuid.uuid4().hex}"
    signature_b64 = signature.encode('utf-8').hex()
    sig_hash = hashlib.md5(signature.encode('utf-8')).hexdigest()
    validation_id = "val_" + uuid.uuid4().hex
    session.sql(f"""
        INSERT INTO DOCGEN.SIGNATURE_VALIDATION (VALIDATION_ID, REQUEST_ID, SIGNER_ID, VALIDATION_STATUS, VALIDATION_HASH, SIGNATURE_BLOB_BASE64, VERIFIED_AT)
        VALUES ('{validation_id}', '{request_id}', '{signer_id}', 'SIGNED_BY_HSM', '{sig_hash}', '{signature_b64}', CURRENT_TIMESTAMP());
    """).collect()
    return {"validation_id": validation_id, "validation_hash": sig_hash}

