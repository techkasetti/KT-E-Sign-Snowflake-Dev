Purpose: call an External Function (HSM-backed signer/verification) via SQL and persist a verification record; this implements the External Function + HSM signer pattern from your PKI notes. @79 @112

# signature_verification_hsm_adapter.py
from snowflake.snowpark import Session
import json, uuid
def verify_signature_with_hsm(session: Session, bundle_id: str):
    """Invoke the HSM/Signer external function and persist verification outcome to DOCGEN.SIGNATURE_VALIDATION."""
    # call external function registered as DOCGEN.HSM_SIGNER_VERIFY
    res = session.sql(f"SELECT DOCGEN.HSM_SIGNER_VERIFY(PARSE_JSON('{{\"bundle_id\":\"{bundle_id}\"}}')) AS res").collect()
    oc = res[0]['RES'] if res else None
    status = oc.get('status') if isinstance(oc, dict) else 'ERROR'
    verification_id = 'verif_' + uuid.uuid4().hex
    session.sql(f"INSERT INTO DOCGEN.SIGNATURE_VALIDATION (VERIFICATION_ID, BUNDLE_ID, STATUS, RESPONSE, VERIFIED_AT) VALUES ('{verification_id}', '{bundle_id}', '{status}', PARSE_JSON('{json.dumps(oc)}'), CURRENT_TIMESTAMP());").collect()
    return {"verification_id": verification_id, "status": status, "raw": oc}

----
