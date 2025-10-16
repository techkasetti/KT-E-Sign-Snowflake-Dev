USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.BILLING_RECON_AUDIT ( AUDIT_ID STRING PRIMARY KEY, RUN_ID STRING, STATUS STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), NOTES STRING ) ; -- @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7188_signature_service_config_keys.sql @31 @24 @1
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SERVICE_CONFIG_KEYS ( KEY_ID STRING PRIMARY KEY, NAME STRING, VALUE VARIANT, SCOPE STRING, UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

