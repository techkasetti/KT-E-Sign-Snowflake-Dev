USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FEATURE_USAGE_LEDGER (
  LEDGER_ID STRING PRIMARY KEY,
  ACCOUNT_ID STRING,
  FEATURE_NAME STRING,
  USAGE_COUNT NUMBER,
  RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1851_signature_feature_usage_ledger.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FEATURE_USAGE_LEDGER (
  LEDGER_ID STRING PRIMARY KEY,
  ACCOUNT_ID STRING,
  FEATURE_NAME STRING,
  USAGE_COUNT NUMBER,
  RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

