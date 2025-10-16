Purpose: call DOCGEN.SLACK_NOTIFY external function via SQL to deliver alerts and operator notifications; this keeps secrets in API_INTEGRATION and follows the External Function usage patterns. @62 @31
# notify_slack.py
from snowflake.snowpark import Session
import json, uuid
def notify_slack(session: Session, alert_id: str):
    """
    Lookup alert details and call DOCGEN.SLACK_NOTIFY external function with the payload.
    """
    rows = session.sql(f"SELECT ALERT_TYPE, PAYLOAD, SEVERITY FROM DOCGEN.ALERTS WHERE ALERT_ID = '{alert_id}'").collect()
    if not rows:
        return {"error":"alert not found"}
    alert = rows[0]
    payload = {
        "text": f"Alert {alert_id}: {alert['ALERT_TYPE']} severity={alert['SEVERITY']}",
        "details": alert['PAYLOAD']
    }
    # call external function
    res = session.sql(f"SELECT DOCGEN.SLACK_NOTIFY(PARSE_JSON('{json.dumps(payload)}')) AS res").collect()
    return {"alert_id": alert_id, "slack_result": res[0]['RES'] if res else None}

