USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.LEGAL_HOLD_NOTIFICATION_QUEUE ( Q_ID STRING PRIMARY KEY, HOLD_ID STRING, TARGET_REF STRING, STATUS STRING DEFAULT 'PENDING', ENQUEUED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/9051_signature_session_termination_jobs.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_TERMINATION_JOBS ( JOB_ID STRING PRIMARY KEY, SESSION_ID STRING, REASON CLOB, STATUS STRING DEFAULT 'PENDING', CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

