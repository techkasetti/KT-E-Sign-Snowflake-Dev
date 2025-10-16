USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_OPS_MISC_2374 ( ID STRING PRIMARY KEY, SUMMARY CLOB, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() )

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2542_signature_account_activity_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ACCOUNT_ACTIVITY_INDEX ( ACT_ID STRING PRIMARY KEY, ACCOUNT_ID STRING, ACTIVITY_TYPE STRING, PAYLOAD VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- @31 @24

