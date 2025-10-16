CREATE OR REPLACE PROCEDURE DOCGEN.CHECK_AND_HANDLE_TIMEOUTS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SESSION_TIMEOUTS (TIMEOUT_ID, SESSION_ID, TIMEOUT_AT) SELECT UUID_STRING(), SESSION_ID, CURRENT_TIMESTAMP() FROM DOCGEN.SESSION_HEARTBEATS WHERE HEARTBEAT_AT < DATEADD('MINUTE', -30, CURRENT_TIMESTAMP());
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2620_signature_session_locks.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_LOCKS (
  LOCK_ID STRING PRIMARY KEY,
  SESSION_ID STRING,
  LOCKED_BY STRING,
  LOCKED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

