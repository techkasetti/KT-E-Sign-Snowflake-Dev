CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_AUTH_AUDIT_05(client_id STRING, action STRING, payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.AUTH_AUDIT_05 (AUDIT_ID, CLIENT_ID, ACTION, PAYLOAD) VALUES (UUID_STRING(), :client_id, :action, :payload);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4046_signature_event_index_06.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_EVENT_INDEX_06 ( SEI_ID STRING PRIMARY KEY, EVENT_REF STRING, EVENT_TYPE STRING, SOURCE_SYSTEM STRING, INDEXED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
