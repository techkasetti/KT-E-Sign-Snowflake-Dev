# notify_pending_alerts.py
# Snowpark stored-proc to query ALERTS table and send email via DOCGEN.SEND_ALERT_EMAIL External Function, then mark alerts resolved.
from snowflake.snowpark import Session
import json

def notify_pending_alerts(session: Session, batch_size: int = 50):
    rows = session.sql(f"SELECT ALERT_ID, ALERT_TYPE, PAYLOAD, SEVERITY FROM DOCGEN.ALERTS WHERE RESOLVED = FALSE LIMIT {batch_size}").collect()
    sent = 0
    for r in rows:
        payload = r['PAYLOAD']
        alert_id = r['ALERT_ID']
        # Call external function to send
        res = session.sql(f"SELECT DOCGEN.SEND_ALERT_EMAIL(PARSE_JSON('{json.dumps(payload)}')) AS res").collect()
        # Mark resolved if successful (demo assumes success)
        session.sql(f"UPDATE DOCGEN.ALERTS SET RESOLVED = TRUE WHERE ALERT_ID = '{alert_id}'").collect()
        sent += 1
    return {"sent": sent}

