CREATE OR REPLACE PROCEDURE DOCGEN.REGISTER_SESSION_REPLAY(session_id STRING, replay_url STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SESSION_REPLAYS (REPLAY_ID, SESSION_ID, REPLAY_URL) VALUES (UUID_STRING(), :session_id, :replay_url);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts output at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3072_signature_session_replay_logs.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_REPLAY_LOGS (
  LOG_ID STRING PRIMARY KEY,
  REPLAY_ID STRING,
  EVENT_TS TIMESTAMP_LTZ,
  EVENT_TYPE STRING,
  PAYLOAD VARIANT,
  LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

