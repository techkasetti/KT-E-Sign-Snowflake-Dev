USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.END_OF_STREAM_MARKER ( MARKER_ID STRING PRIMARY KEY, NOTE STRING, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1466_signature_document_edit_history.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DOCUMENT_EDIT_HISTORY ( EDIT_ID STRING PRIMARY KEY, DOCUMENT_ID STRING, EDITOR STRING, CHANGES VARIANT, EDITED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

