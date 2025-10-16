CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_BATCH_JOB_LOG(job_id STRING, step STRING, details VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.BATCH_JOB_LOGS (LOG_ID, JOB_ID, STEP, DETAILS) VALUES (UUID_STRING(), :job_id, :step, :details);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3788_signature_alert_rules_02.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ALERT_RULES_02 (
  RULE_ID STRING PRIMARY KEY,
  NAME STRING,
  SEVERITY STRING,
  CONDITION VARIANT,
  ACTIONS VARIANT,
  ENABLED BOOLEAN DEFAULT TRUE,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

