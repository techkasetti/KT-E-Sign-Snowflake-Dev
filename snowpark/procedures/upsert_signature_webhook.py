Purpose: Idempotent webhook ingestion handler; inserts event, updates signer/request status, and writes telemetry.
# upsert_signature_webhook.py
from snowflake.snowpark import Session
import uuid, json, datetime

def upsert_signature_webhook(session: Session, raw_event_variant):
    """
    raw_event_variant: dict or JSON variant with fields: event_id, request_id, signer_id, event_type, ts, device, ip, ua
    """
    ev = raw_event_variant
    event_id = ev.get('event_id') or 'evt_' + uuid.uuid4().hex
    request_id = ev.get('request_id')
    signer_id = ev.get('signer_id')
    event_type = ev.get('event_type', 'UNKNOWN')
    event_ts = ev.get('ts') or datetime.datetime.utcnow().isoformat()

    # Idempotent MERGE into SIGNATURE_EVENTS
    session.sql(f"""
        MERGE INTO DOCGEN.SIGNATURE_EVENTS tgt
        USING (SELECT '{event_id}' AS EVENT_ID, PARSE_JSON('{json.dumps(ev)}') AS PAYLOAD, '{event_ts}'::TIMESTAMP_LTZ AS EVENT_TS, '{request_id}' AS REQUEST_ID, '{signer_id}' AS SIGNER_ID) src
        ON tgt.EVENT_ID = src.EVENT_ID
        WHEN MATCHED THEN UPDATE SET PAYLOAD = src.PAYLOAD, EVENT_TS = src.EVENT_TS
        WHEN NOT MATCHED THEN INSERT (EVENT_ID, REQUEST_ID, SIGNER_ID, EVENT_TYPE, EVENT_TS, DEVICE_INFO, IP_ADDR, USER_AGENT, PAYLOAD, INSERTED_AT)
        VALUES (src.EVENT_ID, src.REQUEST_ID, src.SIGNER_ID, '{event_type}', src.EVENT_TS, src.PAYLOAD:device::VARIANT, src.PAYLOAD:ip::STRING, src.PAYLOAD:ua::STRING, src.PAYLOAD, CURRENT_TIMESTAMP());
    """).collect()

    # Update signer status if relevant
    if event_type in ('VIEWED', 'SIGNED', 'REJECTED', 'CANCELLED'):
        session.sql(f"""
            UPDATE DOCGEN.SIGNERS
            SET STATUS = '{'SIGNED' if event_type=='SIGNED' else event_type}', UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE SIGNER_ID = '{signer_id}';
        """).collect()

    # Update request status to COMPLETED when all signers signed
    if event_type == 'SIGNED':
        all_signed = session.sql(f"""
            SELECT COUNT(*) = SUM(CASE WHEN STATUS = 'SIGNED' THEN 1 ELSE 0 END) as all_signed
            FROM DOCGEN.SIGNERS
            WHERE REQUEST_ID = '{request_id}';
        """).collect()
        if all_signed and all_signed[0]['ALL_SIGNED']:
            session.sql(f"""
                UPDATE DOCGEN.SIGNATURE_REQUESTS SET STATUS = 'COMPLETED', UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE REQUEST_ID = '{request_id}';
            """).collect()

    # Insert telemetry row
    session.sql(f"""
        INSERT INTO DOCGEN.SIGNATURE_TELEMETRY (TELEMETRY_ID, REQUEST_ID, ACCOUNT_ID, EVENT_TYPE, LATENCY_MS, DEVICE_TYPE, GEO, CREATED_AT)
        SELECT '{'tel_' + uuid.uuid4().hex}' AS TELEMETRY_ID, '{request_id}', ACCOUNT_ID, '{event_type}', 0, COALESCE(PAYLOAD:device::STRING,'browser'), PARSE_JSON('{{}}')::VARIANT, CURRENT_TIMESTAMP()
        FROM DOCGEN.SIGNATURE_REQUESTS WHERE REQUEST_ID = '{request_id}' LIMIT 1;
    """).collect()

    return {"event_id": event_id, "status": "upserted"}

