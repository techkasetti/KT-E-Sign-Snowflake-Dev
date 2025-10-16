CREATE OR REPLACE PROCEDURE DOCGEN.EMIT_ANALYTICS_EVENT_13(event_id STRING, name STRING, metadata VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIG_ANALYTICS_EVENTS_13 (EVENT_ID, NAME, METADATA) VALUES (:event_id, :name, :metadata);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4952_signature_signer_profiles_14.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNER_PROFILES_14 (
  SIGNER_ID STRING PRIMARY KEY,
  ACCOUNT_ID STRING,
  EMAIL STRING,
  NAME STRING,
  PHONE STRING,
  PROFILE_DATA VARIANT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_LTZ
);

