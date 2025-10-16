INSERT INTO DOCGEN.OPERATOR_ROLES (ROLE_ID, ROLE_NAME, DESCRIPTION) VALUES (UUID_STRING(), 'signature_ops', 'Operator role for signature platform');

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1501_signature_webhook_raw.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_EVENTS_RAW ( RAW_ID STRING PRIMARY KEY, PROVIDER STRING, RAW_PAYLOAD VARIANT, RECEIVED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

