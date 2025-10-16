-- Cleanup seed helper to remove demo rows used in smoke tests; ensures idempotent CI runs. @27 @29
CREATE OR REPLACE PROCEDURE DOCGEN.CLEANUP_DEMO_SEED()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  DELETE FROM DOCGEN.SIGNATURE_EVENTS WHERE EVENT_ID LIKE 'evt-%';
  DELETE FROM DOCGEN.SIGNATURE_REQUESTS WHERE REQUEST_ID LIKE 'req-%';
  RETURN OBJECT_CONSTRUCT('status','cleaned');
$$;

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1211_signature_notifications.sql
-- CREATE table for signer notification preferences and history. @31 @24 @52
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_NOTIFICATIONS (
  NOTIF_ID STRING PRIMARY KEY,
  REQUEST_ID STRING,
  SIGNER_ID STRING,
  CHANNEL STRING,
  STATUS STRING,
  ATTEMPT_COUNT INT DEFAULT 0,
  LAST_ATTEMPT_AT TIMESTAMP_LTZ
);

