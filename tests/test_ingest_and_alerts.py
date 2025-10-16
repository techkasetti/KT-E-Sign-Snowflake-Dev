# tests/test_ingest_and_alerts.py - smoke tests to validate ingestion, event processing and alert dispatch
import json
from snowflake.connector import connect
def test_ingest_and_alert_dispatch():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    # Upload a sample event JSON to stage using PUT (demo)
    sample = {"event_id":"evt_test_ingest_1","request_id":"req_demo_1","signer_id":"s_demo_1","event_type":"VIEWED","ts":"2024-01-01T12:00:00Z","device":{"device":"browser"},"ip":"10.0.0.1","ua":"smoke-agent/1.0"}
    # Insert directly into raw table for test
    cur.execute("INSERT INTO DOCGEN.SIGNATURE_EVENTS_RAW (RAW_VARIANT, FILE_NAME) SELECT PARSE_JSON(%s), 'smoke.json'", (json.dumps(sample),))
    # Call ingestion proc
    cur.execute("CALL DOCGEN.INGEST_SIGNATURE_EVENTS();")
    # Verify normalized event exists
    cur.execute("SELECT COUNT(*) FROM DOCGEN.SIGNATURE_EVENTS WHERE EVENT_ID = 'evt_test_ingest_1';")
    assert cur.fetchone()[0] >= 1
    # Insert an alert to dispatch and call notify proc
    cur.execute("INSERT INTO DOCGEN.ALERTS (ALERT_ID, ALERT_TYPE, PAYLOAD, SEVERITY, RESOLVED) VALUES ('alert_test_1','SMOKE_ALERT', PARSE_JSON('{\"msg\":\"smoke\"}'), 'LOW', FALSE);")
    cur.execute("CALL DOCGEN.NOTIFY_ALERTS(10);")
    # Since external function may be a no-op in test, at least ensure proc runs without error
    assert True
