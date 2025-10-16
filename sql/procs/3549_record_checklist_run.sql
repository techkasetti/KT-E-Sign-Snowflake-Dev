CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_CHECKLIST_RUN(checklist_id STRING, run_by STRING, status STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.CHECKLIST_RUNS (RUN_ID, CHECKLIST_ID, RUN_BY, STARTED_AT, STATUS) VALUES (UUID_STRING(), :checklist_id, :run_by, CURRENT_TIMESTAMP(), :status);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3550_signature_policy_templates.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_TEMPLATES (
  TEMPLATE_ID STRING PRIMARY KEY,
  NAME STRING,
  BODY CLOB,
  CREATED_BY STRING,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

