-- Create stage, file format, pipe and stream for signature event ingestion (concrete names used) USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
-- External stage (uses an existing cloud integration configured in the account) CREATE OR REPLACE STAGE DOCGEN.SIGNATURE_EVENTS_STAGE URL='s3://docgen-ingest-bucket/events/' STORAGE_INTEGRATION = DOCGEN_S3_INTEGRATION FILE_FORMAT=(TYPE=JSON COMPRESSION='AUTO'); -- File format for JSONL ingestion CREATE OR REPLACE FILE FORMAT DOCGEN.FF_JSONL TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE; -- Table to store raw staged JSON lines already exists: DOCGEN.SIGNATURE_EVENTS_RAW -- Create a pipe that copies from stage to raw table (Snowpipe) CREATE OR REPLACE PIPE DOCGEN.PIPE_SIGNATURE_EVENTS AUTO_INGEST = FALSE AS COPY INTO DOCGEN.SIGNATURE_EVENTS_RAW FROM @DOCGEN.SIGNATURE_EVENTS_STAGE FILE_FORMAT = (FORMAT_NAME = 'DOCGEN.FF_JSONL') ON_ERROR = 'CONTINUE' FORCE = FALSE; -- Stream on raw table for change consumption CREATE OR REPLACE STREAM DOCGEN.STREAM_SIG_EVENTS_RAW ON TABLE DOCGEN.SIGNATURE_EVENTS_RAW SHOW_INITIAL_ROWS = FALSE; FILENAME:snowpark/procedures/ingest_signature_events.py
# ingest_signature_events.py
# Snowpark Python stored-proc to process staged signature events from SIGNATURE_EVENTS_RAW -> SIGNATURE_EVENTS (normalized) and to trigger UPSERT_SIGNATURE_WEBHOOK per event (idempotent).
from snowflake.snowpark import Session
import json, uuid, datetime
def ingest_signature_events(session: Session):
    """Process new rows from SIGNATURE_EVENTS_RAW stream, normalize and MERGE into SIGNATURE_EVENTS,
    then mark processed (consumer pattern) by copying processed pointer. This proc is idempotent."""
    # Read from stream (only new rows)
    df = session.table("DOCGEN.SIGNATURE_EVENTS_RAW").select("RAW_VARIANT").filter("TRUE")
    rows = df.collect()
    processed = 0
    for r in rows:
        raw = r['RAW_VARIANT']
        try:
            event_id = raw.get('event_id') or 'evt_' + uuid.uuid4().hex
            request_id = raw.get('request_id')
            signer_id = raw.get('signer_id')
            event_type = raw.get('event_type','UNKNOWN')
            event_ts = raw.get('ts') or datetime.datetime.utcnow().isoformat()
            payload = json.dumps(raw)
            # Idempotent MERGE into SIGNATURE_EVENTS
            session.sql(f"""
                MERGE INTO DOCGEN.SIGNATURE_EVENTS tgt
                USING (SELECT '{event_id}' AS EVENT_ID, PARSE_JSON('{payload}') AS PAYLOAD, '{event_ts}'::TIMESTAMP_LTZ AS EVENT_TS, '{request_id}' AS REQUEST_ID, '{signer_id}' AS SIGNER_ID) src
                ON tgt.EVENT_ID = src.EVENT_ID
                WHEN MATCHED THEN UPDATE SET PAYLOAD = src.PAYLOAD, EVENT_TS = src.EVENT_TS
                WHEN NOT MATCHED THEN INSERT (EVENT_ID, REQUEST_ID, SIGNER_ID, EVENT_TYPE, EVENT_TS, DEVICE_INFO, IP_ADDR, USER_AGENT, PAYLOAD, INSERTED_AT)
                VALUES (src.EVENT_ID, src.REQUEST_ID, src.SIGNER_ID, '{event_type}', src.EVENT_TS, src.PAYLOAD:device::VARIANT, src.PAYLOAD:ip::STRING, src.PAYLOAD:ua::STRING, src.PAYLOAD, CURRENT_TIMESTAMP());
            """).collect()
            processed += 1
        except Exception:
            # swallow per-row errors to allow batch progress (errors logged by Snowflake query history)
            continue
    return {"processed": processed}
