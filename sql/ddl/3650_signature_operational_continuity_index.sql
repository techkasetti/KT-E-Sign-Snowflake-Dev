USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATIONAL_CONTINUITY_INDEX ( IDX_ID STRING PRIMARY KEY, SERVICE_REF STRING, CONTINUITY_JSON VARIANT, UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3501_signature_account_activity_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ACCOUNT_ACTIVITY_INDEX ( ACT_ID STRING PRIMARY KEY, ACCOUNT_ID STRING, ACTIVITY_JSON VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

