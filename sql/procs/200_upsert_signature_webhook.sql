-- Idempotent MERGE-style stored proc registration wrapper that upserts signature webhook payloads into staging and fires ingestion logic per ingest patterns. @14 @11
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_SIGNATURE_WEBHOOK(stage_payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  MERGE INTO DOCGEN.SIGNATURE_EVENTS tgt
  USING (SELECT :stage_payload AS payload) src
  ON tgt.EVENT_ID = src.payload:EVENT_ID::STRING
  WHEN MATCHED THEN
    UPDATE SET EVENT_PAYLOAD = src.payload, EVENT_TS = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (EVENT_ID, REQUEST_ID, DOCUMENT_ID, SIGNER_ID, ACCOUNT_ID, EVENT_TYPE, EVENT_PAYLOAD)
    VALUES (src.payload:EVENT_ID::STRING, src.payload:REQUEST_ID::STRING, src.payload:DOCUMENT_ID::STRING, src.payload:SIGNER_ID::STRING, src.payload:ACCOUNT_ID::STRING, src.payload:EVENT_TYPE::STRING, src.payload);
  RETURN OBJECT_CONSTRUCT('status','ok');
$$;

