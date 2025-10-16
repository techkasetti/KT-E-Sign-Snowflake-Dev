# hsm_sign_via_externalfn.py
# Snowpark stored-proc that delegates binary signing to the HSM External Function and records the result
from snowflake.snowpark import Session
import json, uuid

def hsm_sign_via_externalfn(session: Session, request_id: str, signer_id: str, payload_variant=None):
    """
    Calls the External Function DOCGEN.HSM_SIGN to obtain a PKCS7/DET signature blob and stores verification metadata.
    This is a demo flow: the External Function endpoint returns a JSON object with signature_base64 and cert_chain.
    """
    payload = {
        "request_id": request_id,
        "signer_id": signer_id,
        "payload": payload_variant if payload_variant else {}
    }
    # Call external function (will invoke configured integration gateway)
    df = session.sql(f"SELECT DOCGEN.HSM_SIGN(PARSE_JSON('{json.dumps(payload)}')) AS res").collect()
    res = df[0]['RES'] if df else None
    if not res:
        return {"status": "error", "reason": "no_response"}
    signature_b64 = res.get('signature_base64')
    cert_chain = res.get('cert_chain')
    sign_id = "sign_" + uuid.uuid4().hex
    # Persist signature validation/metadata
    session.sql(f"""
      INSERT INTO DOCGEN.SIGNATURE_VALIDATION (VALIDATION_ID, REQUEST_ID, SIGNER_ID, VALIDATION_STATUS, VALIDATION_HASH, CERT_CHAIN, VERIFIED_AT)
      VALUES ('{sign_id}', '{request_id}', '{signer_id}', 'SIGNED_BY_HSM', '{res.get('hash','')}', PARSE_JSON('{json.dumps(cert_chain)}'), CURRENT_TIMESTAMP());
    """).collect()
    return {"status":"signed","validation_id":sign_id,"signature_b64": signature_b64}

