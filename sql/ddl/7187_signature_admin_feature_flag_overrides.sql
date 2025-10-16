USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_FEATURE_FLAG_OVERRIDES ( OV_ID STRING PRIMARY KEY, FEATURE_KEY STRING, SCOPE_REF STRING, OVERRIDE_JSON VARIANT, APPLIED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7101_signature_api_key_rotation_history.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.API_KEY_ROTATION_HISTORY ( ROTATION_ID STRING PRIMARY KEY, KEY_ID STRING, ROTATED_BY STRING, ROTATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), OLD_KEY_HASH STRING, NEW_KEY_HASH STRING ) ; -- @31

