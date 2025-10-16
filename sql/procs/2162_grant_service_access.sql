CREATE OR REPLACE PROCEDURE DOCGEN.GRANT_SERVICE_ACCESS(service_id STRING, subject STRING, permissions VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SERVICE_ACCESS_CONTROL (ACL_ID, SERVICE_ID, SUBJECT, PERMISSIONS) VALUES (UUID_STRING(), :service_id, :subject, :permissions);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2163_signature_service_tokens.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SERVICE_TOKENS ( TOKEN_ID STRING PRIMARY KEY, SERVICE_ID STRING, TOKEN_HASH STRING, EXPIRES_AT TIMESTAMP_LTZ, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

