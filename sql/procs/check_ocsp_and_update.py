Purpose: poll OCSP/status for certs recorded in PKI_CERTIFICATE_STORE and write/update DOCGEN.PKI_OCSP_STATUS rows; follows PKI/OCSP lifecycle guidance and scheduled OCSP poller task patterns. @176 @97
# check_ocsp_and_update.py
from snowflake.snowpark import Session
import json, uuid, datetime
def check_ocsp_and_update(session: Session, limit:int=100):
    """
    Query recent certs, call OCSP external function (DOCGEN.OCSP_CHECK) for each, and upsert PKI_OCSP_STATUS.
    """
    certs = session.sql("SELECT CERT_ID, FINGERPRINT FROM DOCGEN.PKI_CERTIFICATE_STORE LIMIT %d" % limit).collect()
    results = []
    for c in certs:
        fingerprint = c['FINGERPRINT']
        # call external function that performs OCSP probe (registered separately)
        res = session.sql(f"SELECT DOCGEN.OCSP_CHECK(PARSE_JSON('{{\"fingerprint\":\"{fingerprint}\"}}')) AS res").collect()
        raw = res[0]['RES'] if res else None
        status = raw.get('status') if isinstance(raw, dict) else 'UNKNOWN'
        ocsp_id = 'ocsp_' + uuid.uuid4().hex
        session.sql(f"""
            MERGE INTO DOCGEN.PKI_OCSP_STATUS t
            USING (SELECT '{fingerprint}' AS CERT_FINGERPRINT, PARSE_JSON('{json.dumps(raw)}') AS OCSP_RESPONSE, '{status}' AS STATUS, CURRENT_TIMESTAMP() AS CHECKED_AT) s
            ON t.CERT_FINGERPRINT = s.CERT_FINGERPRINT
            WHEN MATCHED THEN UPDATE SET OCSP_RESPONSE = s.OCSP_RESPONSE, STATUS = s.STATUS, CHECKED_AT = s.CHECKED_AT
            WHEN NOT MATCHED THEN INSERT (OCSP_ID, CERT_FINGERPRINT, OCSP_RESPONSE, STATUS, CHECKED_AT) VALUES ('{ocsp_id}', s.CERT_FINGERPRINT, s.OCSP_RESPONSE, s.STATUS, s.CHECKED_AT);
        """).collect()
        results.append({"fingerprint": fingerprint, "status": status})
    return {"checked": len(results), "details": results}

