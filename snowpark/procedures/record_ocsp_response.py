from snowflake.snowpark import Session
def record_ocsp_response(session: Session, cert_fingerprint, ocsp_response):
    session.sql(f"INSERT INTO DOCGEN.OCSP_RESPONSES (OCSP_ID, CERT_FINGERPRINT, OCSP_STATUS, OCSP_RAW, CHECKED_AT) VALUES (UUID_STRING(), '{cert_fingerprint}', '{ocsp_response.get('status')}', PARSE_JSON('{json.dumps(ocsp_response)}'), CURRENT_TIMESTAMP())").collect()
    return {"status":"ok"}
# Records OCSP snapshots used by verification SPs and evidence bundles @68 @101

