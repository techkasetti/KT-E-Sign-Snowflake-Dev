USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_SATISFACTION_METRICS ( MET_ID STRING PRIMARY KEY, SURV_ID STRING, METRICS VARIANT, AGG_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8131_signature_api_key_rotation_audit.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.API_KEY_ROTATION_AUDIT ( AUDIT_ID STRING PRIMARY KEY, KEY_ID STRING, ACTION STRING, ACTOR STRING, ACTION_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), DETAILS VARIANT );

