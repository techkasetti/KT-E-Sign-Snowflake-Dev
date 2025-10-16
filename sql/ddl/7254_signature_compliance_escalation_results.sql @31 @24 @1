USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.COMPLIANCE_ESCALATION_RESULTS ( RES_ID STRING PRIMARY KEY, Q_ID STRING, HANDLED_BY STRING, RESOLUTION JSON, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7255_signature_manual_review_templates.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.MANUAL_REVIEW_TEMPLATES (
  TEMPLATE_ID STRING PRIMARY KEY,
  NAME STRING,
  VERSION INT,
  BODY CLOB,
  CREATED_BY STRING,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
-- @31

