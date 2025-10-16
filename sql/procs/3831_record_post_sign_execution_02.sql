CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_POST_SIGN_EXECUTION_02(hook_id STRING, bundle_id STRING, status STRING, result VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.POST_SIGN_EXECUTIONS_02 (EXEC_ID, HOOK_ID, BUNDLE_ID, STATUS, RESULT) VALUES (UUID_STRING(), :hook_id, :bundle_id, :status, :result);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3832_signature_export_jobs_03.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.EXPORT_JOBS_03 ( JOB_ID STRING PRIMARY KEY, JOB_TYPE STRING, TARGET_LOCATION STRING, STATUS STRING DEFAULT 'PENDING', CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), META VARIANT );

