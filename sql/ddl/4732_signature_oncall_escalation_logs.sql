USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ONCALL_ESCALATION_LOGS ( LOG_ID STRING PRIMARY KEY, INCIDENT_ID STRING, ESCALATION_JSON VARIANT, ESCALATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @1

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4733_signature_workflow_event_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.WORKFLOW_EVENT_INDEX ( IDX_ID STRING PRIMARY KEY, WORKFLOW_REF STRING, EVENT_TYPE STRING, EVENT_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31
