USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DOCGEN_OPERATIONAL_NOTES ( NOTE_ID STRING PRIMARY KEY, CATEGORY STRING, NOTE CLOB, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7058_signature_document_qos_profiles.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN; CREATE OR REPLACE TABLE DOCGEN.DOCUMENT_QOS_PROFILES ( PROFILE_ID STRING PRIMARY KEY, NAME STRING, SLO JSON, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24
