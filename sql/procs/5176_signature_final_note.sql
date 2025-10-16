-- End of tranche: these artifacts implement a complete Snowflake-side E-Sign surface (DDL, SPs, TASKs, views, and registration templates) ready for staging and registration; replace environment placeholders and register per the runbook.

Final guidance on registration and placeholder replacement per runbook patterns. @78 @31



- User: [[mention:xyq669x4l64mk5a4aos4pdk6:SnowflakeESignV11txt:DOCUMENT]] Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/000_signature_requests.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_REQUESTS (
  REQUEST_ID STRING PRIMARY KEY,
  DOCUMENT_ID STRING,
  ACCOUNT_ID STRING,
  CREATED_BY STRING,
  STATUS STRING,
  REQUIRED_AUTH_LEVEL STRING,
  POLICY_ID STRING,
  META VARIANT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
); Generated per Snowflake E-Sign patterns. @31 @24

