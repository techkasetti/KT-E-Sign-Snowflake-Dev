USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_ADHERENCE_REPORTS ( REPORT_ID STRING PRIMARY KEY, OPERATOR_REF STRING, REPORT_JSON VARIANT, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4301_signature_agent_config.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.AGENT_CONFIG (
  AGENT_ID STRING PRIMARY KEY,
  NAME STRING,
  VERSION STRING,
  CONFIG VARIANT,
  ENABLED BOOLEAN DEFAULT TRUE,
  UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
-- Generated per Snowflake E-Sign DDL patterns. @31 @24

