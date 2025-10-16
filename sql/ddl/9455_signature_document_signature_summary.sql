USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DOCUMENT_SIGNATURE_SUMMARY ( SUM_ID STRING PRIMARY KEY, DOCUMENT_ID STRING, SIGNATURE_COUNT INT, LAST_SIGNED_AT TIMESTAMP_LTZ );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/9456_signature_statement_templates.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_STATEMENT_TEMPLATES ( TEMPLATE_ID STRING PRIMARY KEY, NAME STRING, BODY CLOB, CREATED_BY STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

