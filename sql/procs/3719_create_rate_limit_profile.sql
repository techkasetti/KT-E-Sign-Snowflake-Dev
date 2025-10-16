CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_RATE_LIMIT_PROFILE(name STRING, limits VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.RATE_LIMIT_PROFILES (PROFILE_ID, NAME, LIMITS) VALUES (UUID_STRING(), :name, :limits);
$$
-- @1

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3680_signatory_keys_01.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATORY_KEYS_01 (
  KEY_ID STRING PRIMARY KEY,
  SIGNATORY_REF STRING,
  KEY_TYPE STRING,
  KEY_PEM CLOB,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  EXPIRES_AT TIMESTAMP_LTZ,
  META VARIANT
);

