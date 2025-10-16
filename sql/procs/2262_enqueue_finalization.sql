CREATE OR REPLACE PROCEDURE DOCGEN.ENQUEUE_FINALIZATION(bundle_id STRING, next_attempt_at TIMESTAMP_LTZ)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.FINALIZATION_QUEUE (FQ_ID, BUNDLE_ID, NEXT_ATTEMPT_AT) VALUES (UUID_STRING(), :bundle_id, :next_attempt_at);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2263_signature_finalization_logs.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FINALIZATION_LOGS (
  LOG_ID STRING PRIMARY KEY,
  FQ_ID STRING,
  STATUS STRING,
  DETAILS VARIANT,
  LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

