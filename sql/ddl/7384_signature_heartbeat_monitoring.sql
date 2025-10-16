USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.HEARTBEAT_MONITORING (
  HB_ID STRING PRIMARY KEY,
  COMPONENT STRING,
  STATUS STRING,
  LAST_HEARTBEAT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
-- @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7385_signature_template_review_assignments.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
-- Generated per Snowflake E-Signature patterns @31 @24 @56
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_REVIEW_ASSIGNMENTS (
  ASSIGN_ID STRING PRIMARY KEY,
  TEMPLATE_ID STRING,
  REVIEWER_REF STRING,
  ASSIGNED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

