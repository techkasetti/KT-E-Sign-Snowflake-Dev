USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_COMPETENCY_METRICS ( MET_ID STRING PRIMARY KEY, OPERATOR STRING, METRIC_NAME STRING, METRIC_VALUE VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() )

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/401_signature_request_history.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_REQUEST_HISTORY (
  HIST_ID STRING PRIMARY KEY,
  REQUEST_ID STRING,
  OLD_STATUS STRING,
  NEW_STATUS STRING,
  CHANGED_BY STRING,
  CHANGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
-- Generated per Snowflake E-Sign patterns @31 @24 @56

