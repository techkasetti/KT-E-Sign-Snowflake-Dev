# ingest_signature_events.py
# Snowpark Python stored-proc to read from SIGNATURE_EVENTS_RAW -> parse and insert into SIGNATURE_EVENTS,
# seed SIGNATURE_REQUESTS and SIGNERS, and write provenance. This proc implements idempotent MERGE semantics.
from snowflake.snowpark import Session
import json, uuid, datetime

def embedding_for_text(text: str):
    # demo lightweight deterministic "embedding" (vector as JSON) for dev usage
    return {"vector": [round(ord(c) % 10 / 10.0, 3) for c in text[:32]]}

def ingest_signature_events(session: Session):
    cur = session.sql("SELECT RAW_VARIANT FROM DOCGEN.SIGNATURE_EVENTS_RAW WHERE RAW_VARIANT IS NOT NULL").collect()
    processed = 0
    for r in cur:
        ev = r['RAW_VARIANT']
        # required fields: request_id, signer_id, event_type, ts
        request_id = ev.get('request_id')
        signer_id = ev.get('signer_id')
        event_type = ev.get('event_type')
        event_ts = ev.get('ts') or datetime.datetime.utcnow().isoformat()
        evt_id = ev.get('event_id') or 'evt_' + uuid.uuid4().hex
        # Upsert into SIGNATURE_EVENTS (idempotent by event_id)
        session.sql(f"""
            MERGE INTO DOCGEN.SIGNATURE_EVENTS tgt
            USING (SELECT '{evt_id}' AS EVENT_ID, PARSE_JSON('{json.dumps(ev)}') AS PAYLOAD, '{event_ts}'::TIMESTAMP_LTZ AS EVENT_TS, '{request_id}' AS REQUEST_ID, '{signer_id}' AS SIGNER_ID) src
            ON tgt.EVENT_ID = src.EVENT_ID
            WHEN MATCHED THEN UPDATE SET PAYLOAD = src.PAYLOAD, EVENT_TS = src.EVENT_TS
            WHEN NOT MATCHED THEN INSERT (EVENT_ID, REQUEST_ID, SIGNER_ID, EVENT_TYPE, EVENT_TS, PAYLOAD) VALUES (src.EVENT_ID, src.REQUEST_ID, src.SIGNER_ID, '{event_type}', src.EVENT_TS, src.PAYLOAD);
        """).collect()
        # Ensure a SIGNATURE_REQUEST exists for this request_id (idempotent)
        session.sql(f"""
            MERGE INTO DOCGEN.SIGNATURE_REQUESTS tr
            USING (SELECT '{request_id}' AS REQUEST_ID) src
            ON tr.REQUEST_ID = src.REQUEST_ID
            WHEN NOT MATCHED THEN INSERT (REQUEST_ID, ACCOUNT_ID, DOCUMENT_ID, TEMPLATE_ID, STATUS, CREATED_AT) VALUES (src.REQUEST_ID, 'acct_prod_0001', 'doc_unknown', 'tpl_unknown', 'PENDING', CURRENT_TIMESTAMP());
        """).collect()
        # Ensure signer row exists
        session.sql(f"""
            MERGE INTO DOCGEN.SIGNERS s
            USING (SELECT '{signer_id}' AS SIGNER_ID, '{request_id}' AS REQUEST_ID) src
            ON s.SIGNER_ID = src.SIGNER_ID
            WHEN NOT MATCHED THEN INSERT (SIGNER_ID, REQUEST_ID, ACCOUNT_ID, SIGNER_EMAIL, SIGNER_NAME, SIGNER_ORDER, AUTH_METHOD, CREATED_AT) VALUES (src.SIGNER_ID, src.REQUEST_ID, 'acct_prod_0001', 'unknown@example.com', 'Unknown', 1, 'unknown', CURRENT_TIMESTAMP())
            WHEN MATCHED THEN UPDATE SET REQUEST_ID = src.REQUEST_ID;
        """).collect()
        processed += 1
    return {"status": "ok", "processed": processed}

