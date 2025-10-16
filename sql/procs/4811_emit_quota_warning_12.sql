CREATE OR REPLACE PROCEDURE DOCGEN.EMIT_QUOTA_WARNING_12(warning_id STRING, account_id STRING, feature_code STRING, usage NUMBER, limit NUMBER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.QUOTA_WARNINGS_12 (WARNING_ID, ACCOUNT_ID, FEATURE_CODE, USAGE, LIMIT) VALUES (:warning_id, :account_id, :feature_code, :usage, :limit);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4812_signature_api_keys_13.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.API_KEYS_13 (
  KEY_ID STRING PRIMARY KEY,
  ACCOUNT_ID STRING,
  KEY_HASH STRING,
  SCOPE VARIANT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

