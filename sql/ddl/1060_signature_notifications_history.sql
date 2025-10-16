-- Emitted per Snowflake_E-Sign_V1.2 patterns and registration order. @1 @3
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_NOTIFICATIONS_HISTORY (
  NH_ID STRING PRIMARY KEY,
  NOTIF_ID STRING,
  STATUS STRING,
  SENT_AT TIMESTAMP_LTZ,
  DETAILS VARIANT
);

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: snowpark/procedures/ups ert_signature_webhook.py
# UPSERT_SIGNATURE_WEBHOOK(stage_payload VARIANT) - idempotent MERGE-style ingestion
from snowflake.snowpark import Session
import uuid, json

def upsert_signature_webhook(session: Session, stage_payload):
    data = stage_payload
    req_id = data.get('request_id') or str(uuid.uuid4())
    session.sql(f"""
        MERGE INTO DOCGEN.SIGNATURE_EVENTS tgt
        USING (SELECT PARSE_JSON('{json.dumps(data)}') AS payload) src
        ON tgt.EVENT_ID = src.payload:event_id::STRING
        WHEN NOT MATCHED THEN INSERT (EVENT_ID, REQUEST_ID, DOCUMENT_ID, SIGNER_ID, ACCOUNT_ID, EVENT_TYPE, EVENT_PAYLOAD, EVENT_TS)
        VALUES (src.payload:event_id::STRING, src.payload:request_id::STRING, src.payload:document_id::STRING, src.payload:signer_id::STRING, src.payload:account_id::STRING, src.payload:event_type::STRING, src.payload, CURRENT_TIMESTAMP());
    """).collect()
    return {"status": "ok", "request_id": req_id}
# Idempotent MERGE ingestion pattern follows repo ingestion/merge patterns @31 @243

