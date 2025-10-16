Purpose: Periodic analytics job to compute signature completion rates, latencies and generate telemetry aggregates; writes to ALERTS for anomalies.
# run_signature_analytics.py
from snowflake.snowpark import Session
import uuid, json

def run_signature_analytics(session: Session):
    # Compute completion rate last 24 hours
    completion = session.sql("""
        SELECT COUNT(*) AS total_requests,
               SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS completed
        FROM DOCGEN.SIGNATURE_REQUESTS
        WHERE CREATED_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP());
    """).collect()[0]
    total = completion['TOTAL_REQUESTS'] or 0
    completed = completion['COMPLETED'] or 0
    rate = (completed / total * 100) if total > 0 else 0
    # If low completion rate, create an alert
    if total > 0 and rate < 40:
        alert_id = "alert_" + uuid.uuid4().hex
        payload = {"metric": "completion_rate", "value": rate, "period": "24h"}
        session.sql(f"""
            INSERT INTO DOCGEN.ALERTS (ALERT_ID, ALERT_TYPE, PAYLOAD, SEVERITY)
            VALUES ('{alert_id}', 'SIGNATURE_COMPLETION_LOW', PARSE_JSON('{json.dumps(payload)}'), 'HIGH');
        """).collect()
    # Return computed stats
    return {"total_requests": total, "completed": completed, "completion_rate": rate}

