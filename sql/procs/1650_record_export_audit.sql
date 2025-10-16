CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_EXPORT_AUDIT(entity_ref STRING, export_type STRING, target STRING, requested_by STRING, status STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.EXPORT_AUDIT (EXPORT_AUDIT_ID, ENTITY_REF, EXPORT_TYPE, TARGET, REQUESTED_BY, STATUS) VALUES (UUID_STRING(), :entity_ref, :export_type, :target, :requested_by, :status);
$$;

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1651_signature_session_tokens.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_TOKENS (TOKEN_ID STRING PRIMARY KEY, SESSION_ID STRING, TOKEN STRING, EXPIRES_AT TIMESTAMP_LTZ, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()); 
-- @31 @24 @52

