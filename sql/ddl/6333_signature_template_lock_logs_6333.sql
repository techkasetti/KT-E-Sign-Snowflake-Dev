USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_LOCK_LOGS_6333 ( LOG_ID STRING PRIMARY KEY, LOCK_ID STRING, ENTRY VARIANT, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6334_signature_template_fork_requests_6334.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_FORK_REQUESTS_6334 ( REQ_ID STRING PRIMARY KEY, TEMPLATE_ID STRING, FORK_BY STRING, STATUS STRING DEFAULT 'PENDING', REQUESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

