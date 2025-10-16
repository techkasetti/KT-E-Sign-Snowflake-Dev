CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_CI_CD_RUN(pipeline STRING, status STRING, started_at TIMESTAMP_LTZ, ended_at TIMESTAMP_LTZ, meta VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.CI_CD_RUNS (RUN_ID, PIPELINE, STATUS, STARTED_AT, ENDED_AT, META) VALUES (UUID_STRING(), :pipeline, :status, :started_at, :ended_at, :meta);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2339_signature_lock_table.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OBJECT_LOCKS (
  LOCK_ID STRING PRIMARY KEY,
  OBJECT_REF STRING,
  LOCKED_BY STRING,
  LOCKED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  EXPIRES_AT TIMESTAMP_LTZ
);

