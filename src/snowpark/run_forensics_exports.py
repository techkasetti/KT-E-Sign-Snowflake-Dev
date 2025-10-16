def handler(session):
    rows = session.sql("SELECT EXPORT_ID FROM DOCGEN.FORENSICS_EXPORTS WHERE STATUS='PENDING'").collect()
    return {"exports": len(rows)}

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1926_signature_retry_queue.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.RETRY_QUEUE (
  QUEUE_ID STRING PRIMARY KEY,
  TARGET_PROC STRING,
  PAYLOAD VARIANT,
  ATTEMPTS INT DEFAULT 0,
  NEXT_ATTEMPT_AT TIMESTAMP_LTZ,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

