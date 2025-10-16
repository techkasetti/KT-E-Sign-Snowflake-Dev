from snowflake.snowpark import Session
def detect_signature_anomalies(session: Session):
    # simple anomaly detection: signatures per minute threshold
    session.sql("INSERT INTO DOCGEN.ANOMALY_ALERTS (ALERT_ID, ENTITY_REF, ALERT_TYPE, SCORE, DETAILS, CREATED_AT) SELECT UUID_STRING(), ACCOUNT_ID, 'HIGH_VELOCITY', 0.9, PARSE_JSON('{}'), CURRENT_TIMESTAMP() FROM DOCGEN.SIGNATURE_EVENTS GROUP BY ACCOUNT_ID HAVING COUNT(*) > 100").collect()
    return {"status":"ok"}
# Anomaly detector writes alerts per signature analytics patterns @2363 @3150

