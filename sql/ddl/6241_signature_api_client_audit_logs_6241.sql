USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.API_CLIENT_AUDIT_LOGS_6241 ( LOG_ID STRING PRIMARY KEY, AUDIT_ID STRING, ENTRY VARIANT, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6242_signature_user_device_authn_6242.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.USER_DEVICE_AUTHN_6242 ( AUTHN_ID STRING PRIMARY KEY, USER_REF STRING, DEVICE_ID STRING, AUTHN_METHOD STRING, AUTHN_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), META VARIANT ); @31

