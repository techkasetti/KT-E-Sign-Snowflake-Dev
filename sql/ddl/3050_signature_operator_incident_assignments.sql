USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_INCIDENT_ASSIGNMENTS ( ASSIGN_ID STRING PRIMARY KEY, INCIDENT_ID STRING, OPERATOR_REF STRING, ASSIGNED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3051_signature_task_execution_audit.sql @31 @24
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN; CREATE OR REPLACE TABLE DOCGEN.TASK_EXECUTION_AUDIT ( AUDIT_ID STRING PRIMARY KEY, TASK_NAME STRING, STATUS STRING, STARTED_AT TIMESTAMP_LTZ, ENDED_AT TIMESTAMP_LTZ, DETAILS VARIANT ); @31 @24

