USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATIONAL_REVIEW_NOTES ( NOTE_ID STRING PRIMARY KEY, REVIEW_ID STRING, AUTHOR STRING, NOTE CLOB, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8001_signature_operational_review_followups.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATIONAL_REVIEW_FOLLOWUPS ( FOLLOWUP_ID STRING PRIMARY KEY, REVIEW_ID STRING, ACTION_JSON VARIANT, STATUS STRING DEFAULT 'PENDING', CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

