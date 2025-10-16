Purpose: detect anomalies (missing evidence, export mismatches, repeated failures) and write DOCGEN.ALERTS rows; this supports operator alert flow and Task usage described in runbooks @118 @216.  
# alert_detector.py
from snowflake.snowpark import Session
import uuid
def alert_detector(session: Session, max_missing: int = 10):
    """Detect evidence bundles without archive_location and emit alerts for ops."""
    rows = session.sql("SELECT BUNDLE_ID FROM DOCGEN.EVIDENCE_BUNDLE WHERE ARCHIVE_LOCATION IS NULL LIMIT 100").collect()
    alerts = 0
    for r in rows:
        alert_id = "alert_" + uuid.uuid4().hex
        session.sql(f"INSERT INTO DOCGEN.ALERTS (ALERT_ID, ALERT_TYPE, PAYLOAD, SEVERITY, RESOLVED, ALERT_TS) VALUES ('{alert_id}', 'MISSING_ARCHIVE', PARSE_JSON('{{\"bundle_id\":\"{r['BUNDLE_ID']}\"}}'), 'HIGH', FALSE, CURRENT_TIMESTAMP());").collect()
        alerts += 1
    return {"alerts_created": alerts}  

