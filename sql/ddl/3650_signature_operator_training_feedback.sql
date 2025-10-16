USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_TRAINING_FEEDBACK ( FB_ID STRING PRIMARY KEY, SCHEDULE_ID STRING, OPERATOR_REF STRING, FEEDBACK CLOB, SUBMITTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() )

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3651_signature_alert_feedback_queue.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ALERT_FEEDBACK_QUEUE ( QUEUE_ID STRING PRIMARY KEY, ALERT_ID STRING, FEEDBACK JSON, STATUS STRING DEFAULT 'PENDING', ENQUEUED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31
