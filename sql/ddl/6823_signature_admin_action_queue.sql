USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN; CREATE OR REPLACE TABLE DOCGEN.ADMIN_ACTION_QUEUE ( ACTION_ID STRING PRIMARY KEY, ACTION_JSON VARIANT, STATUS STRING DEFAULT 'PENDING', SCHEDULED_AT TIMESTAMP_LTZ, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6824_signature_admin_escalation_notifications.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_ESCALATION_NOTIFS ( NOTIF_ID STRING PRIMARY KEY, INCIDENT_ID STRING, LEVEL STRING, MESSAGE STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

