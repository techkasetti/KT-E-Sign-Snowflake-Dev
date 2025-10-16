USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.AGENT_ACTIVITY_LOGS ( LOG_ID STRING PRIMARY KEY, AGENT_REF STRING, ACTIVITY_JSON VARIANT, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4251_signature_customer_satisfaction_surveys.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CUSTOMER_SATISFACTION_SURVEYS ( SURVEY_ID STRING PRIMARY KEY, TENANT_ID STRING, SCORE NUMBER, RESPONSES VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

