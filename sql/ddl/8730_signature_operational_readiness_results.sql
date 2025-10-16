USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATIONAL_READINESS_RESULTS ( RES_ID STRING PRIMARY KEY, CHECK_ID STRING, STATUS STRING, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8731_signature_operator_availability_calendar.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_AVAILABILITY_CALENDAR ( CAL_ID STRING PRIMARY KEY, OP_REF STRING, START_AT TIMESTAMP_LTZ, END_AT TIMESTAMP_LTZ, AVAILABILITY JSON, UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @24 @31 @56

