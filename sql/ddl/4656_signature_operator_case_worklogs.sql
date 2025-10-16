USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_CASE_WORKLOGS ( LOG_ID STRING PRIMARY KEY, CASE_REF STRING, OPERATOR_REF STRING, WORK_JSON VARIANT, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4657_signature_template_retirement_schedule.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_RETIREMENT_SCHEDULE ( SCHEDULE_ID STRING PRIMARY KEY, TEMPLATE_ID STRING, RETIRE_AT TIMESTAMP_LTZ, REASON CLOB, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @1
