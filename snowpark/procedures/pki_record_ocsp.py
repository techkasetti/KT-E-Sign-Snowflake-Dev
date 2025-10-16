Purpose: Snowpark stored procedure to record OCSP responses for certificates (HSM calls remain external). @115 @173

# pki_record_ocsp.py
from snowflake.snowpark import Session
import uuid, json

def pki_record_ocsp(session: Session, cert_id: str, ocsp_response_variant):
    ocsp_id = "ocsp_" + str(uuid.uuid4())
    session.sql(f"""
        INSERT INTO DOCGEN.PKI_OCSP_STATUS (OCSP_ID, CERT_ID, OCSP_RESPONSE, STATUS, CHECKED_AT)
        VALUES ('{ocsp_id}', '{cert_id}', PARSE_JSON('{json.dumps(ocsp_response_variant)}'), '{ocsp_response_variant.get('status','unknown')}', CURRENT_TIMESTAMP());
    """).collect()
    return {"ocsp_id": ocsp_id, "status": "recorded"}

