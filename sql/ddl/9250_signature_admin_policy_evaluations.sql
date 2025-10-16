USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_POLICY_EVALUATIONS ( EVAL_ID STRING PRIMARY KEY, INSTANCE_ID STRING, RESULT JSON, EVALUATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @24 @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/9251_signature_session_archive.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_ARCHIVE ( ARCHIVE_ID STRING PRIMARY KEY, SESSION_ID STRING, PAYLOAD VARIANT, ARCHIVED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

