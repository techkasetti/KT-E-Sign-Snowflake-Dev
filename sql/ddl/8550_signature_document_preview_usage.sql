USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DOCUMENT_PREVIEW_USAGE ( USG_ID STRING PRIMARY KEY, PREVIEW_ID STRING, USER_REF STRING, VIEWED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @24 @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8551_signature_operational_runbook_entries.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATIONAL_RUNBOOK_ENTRIES ( ENTRY_ID STRING PRIMARY KEY, TITLE STRING, BODY CLOB, OWNER STRING, UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- @31 @24
