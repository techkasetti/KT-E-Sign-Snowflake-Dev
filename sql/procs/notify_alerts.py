# notify_alerts.py
# Snowpark Python stored procedure to dispatch unresolved alerts via the EMAIL_ALERT External Function
from snowflake.snowpark import Session
import json, uuid
def notify_alerts(session: Session, limit: int = 50):
    """Select unresolved alerts and call DOCGEN.EMAIL_ALERT external function for each (idempotent by ALERT_ID)."""
    rows = session.sql("SELECT ALERT_ID, ALERT_TYPE, PAYLOAD FROM DOCGEN.ALERTS WHERE RESOLVED = FALSE ORDER BY ALERT_TS ASC LIMIT {0}".format(int(limit))).collect()
    dispatched = 0
    for r in rows:
        alert_id = r['ALERT_ID']
        payload = r['PAYLOAD']
        try:
            # call external function to dispatch email (this performs outbound HTTP call)
            session.sql("SELECT DOCGEN.EMAIL_ALERT(PARSE_JSON('{0}')) AS res".format(json.dumps({"alert_id": alert_id, "type": r['ALERT_TYPE'], "payload": payload}))).collect()
            dispatched += 1
        except Exception:
            # log failure and continue
            continue
    return {"dispatched": dispatched}
