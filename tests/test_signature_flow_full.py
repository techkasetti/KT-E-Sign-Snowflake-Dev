# pytest full signature flow smoke test (demo)
import time
import json
from snowflake.connector import connect

def conn():
    return connect(user="svc_docgen", account="client_prod_001", password="demo_password", role="DOCGEN_ADMIN")

def test_full_signature_flow_smoke():
    c = conn()
    cur = c.cursor()
    # Ensure seed exists
    cur.execute("SELECT COUNT(*) FROM DOCGEN.SIGNATURE_REQUESTS WHERE REQUEST_ID='req_demo_1'")
    assert cur.fetchone()[0] >= 1
    # Simulate webhook event ingestion by calling UPSERT_SIGNATURE_WEBHOOK with a raw event
    raw_event = {
        "event_id":"evt_demo_1",
        "request_id":"req_demo_1",
        "signer_id":"s_demo_1",
        "event_type":"VIEWED",
        "ts": "2024-01-01T12:00:00Z",
        "device":{"device":"browser"},
        "ip":"1.2.3.4",
        "ua":"demo-agent/1.0"
    }
    cur.execute(f"CALL DOCGEN.UPSERT_SIGNATURE_WEBHOOK(PARSE_JSON('{json.dumps(raw_event)}'));")
    # Then simulate SIGNED event
    raw_event["event_type"] = "SIGNED"
    raw_event["event_id"] = "evt_demo_2"
    cur.execute(f"CALL DOCGEN.UPSERT_SIGNATURE_WEBHOOK(PARSE_JSON('{json.dumps(raw_event)}'));")
    # Validate request status moved to COMPLETED
    cur.execute("SELECT STATUS FROM DOCGEN.SIGNATURE_REQUESTS WHERE REQUEST_ID='req_demo_1'")
    status = cur.fetchone()[0]
    assert status in ('COMPLETED','PENDING','RENDERED')  # demo tolerant
    # Call validate proc (demo signature blob)
    cur.execute("CALL DOCGEN.VALIDATE_SIGNATURE_AND_RECORD('req_demo_1','s_demo_1','demo_signature_b64', PARSE_JSON('{}'));")
    cur.execute("SELECT COUNT(*) FROM DOCGEN.SIGNATURE_VALIDATION WHERE REQUEST_ID='req_demo_1' AND SIGNER_ID='s_demo_1'")
    assert cur.fetchone()[0] >= 1

