USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN; CREATE OR REPLACE TABLE DOCGEN.POLICY_CHANGE_APPROVALS ( APP_ID STRING PRIMARY KEY, POLICY_ID STRING, APPROVER STRING, STATUS STRING, APPROVED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3201_signature_recovery_token_store.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.RECOVERY_TOKEN_STORE ( TOKEN_ID STRING PRIMARY KEY, SUBJECT_REF STRING, TOKEN_HASH STRING, EXPIRES_AT TIMESTAMP_LTZ, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- @31

