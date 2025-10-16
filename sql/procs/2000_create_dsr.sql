CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_DSR(subject_id STRING, request_type STRING, scope VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.DATA_SUBJECT_REQUESTS (DSR_ID, SUBJECT_ID, REQUEST_TYPE, SCOPE, STATUS) VALUES (UUID_STRING(), :subject_id, :request_type, :scope, 'OPEN');
$$;

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2001_signature_dsr_audit.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DSR_AUDIT ( AUDIT_ID STRING PRIMARY KEY, DSR_ID STRING, ACTION STRING, PERFORMED_BY STRING, META VARIANT, ACTION_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

