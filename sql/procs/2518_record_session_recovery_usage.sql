CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_SESSION_RECOVERY_USAGE(token_id STRING, used_by STRING, meta VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SESSION_RECOVERY_USAGE (USG_ID, TOKEN_ID, USED_BY, META) VALUES (UUID_STRING(), :token_id, :used_by, :meta);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2445_signature_provider_credentials.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PROVIDER_CREDENTIALS (
  CRED_ID STRING PRIMARY KEY,
  PROVIDER_ID STRING,
  CRED_TYPE STRING,
  CRED_META VARIANT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

