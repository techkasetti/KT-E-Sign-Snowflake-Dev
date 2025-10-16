USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_AUDIT_INDEX_03 ( IDX_ID STRING PRIMARY KEY, AUDIT_ID STRING, ENTITY_REF STRING, INDEXED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User:  

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4101_signature_admin_alert_acknowledgements.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ALERT_ACKNOWLEDGEMENTS ( ACK_ID STRING PRIMARY KEY, ALERT_ID STRING, ACKED_BY STRING, ACKED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), NOTES CLOB ); @31 @24

