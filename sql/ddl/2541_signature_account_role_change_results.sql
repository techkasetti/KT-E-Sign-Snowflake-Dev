USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ACCOUNT_ROLE_CHANGE_RESULTS ( RES_ID STRING PRIMARY KEY, REQ_ID STRING, OUTCOME VARIANT, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2301_signature_ops_misc_2301.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_OPS_MISC_2301 ( ID STRING PRIMARY KEY, KEY_NAME STRING, META VARIANT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

