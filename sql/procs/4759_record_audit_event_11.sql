CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_AUDIT_EVENT_11(event_id STRING, config_id STRING, payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.AUDIT_EVENTS_11 (EVENT_ID, CONFIG_ID, PAYLOAD) VALUES (:event_id, :config_id, :payload);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4760_signature_system_events_12.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SYSTEM_EVENTS_12 (
  EVENT_ID STRING PRIMARY KEY,
  SOURCE STRING,
  LEVEL STRING,
  PAYLOAD VARIANT,
  OCCURRED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

