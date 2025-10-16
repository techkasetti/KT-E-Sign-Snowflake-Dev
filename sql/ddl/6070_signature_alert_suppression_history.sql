USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ALERT_SUPPRESSION_HISTORY ( H_ID STRING PRIMARY KEY, SUPPRESS_ID STRING, ALERT_ID STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), EXPIRES_AT TIMESTAMP_LTZ );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6071_signature_operator_incident_priorities.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_INCIDENT_PRIORITIES ( PRIORITY_ID STRING PRIMARY KEY, INCIDENT_ID STRING, PRIORITY_LEVEL STRING, SCORE NUMBER, SET_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

