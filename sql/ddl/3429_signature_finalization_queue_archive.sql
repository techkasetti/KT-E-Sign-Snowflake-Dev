USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FINALIZATION_QUEUE_ARCHIVE ( ARCHIVE_ID STRING PRIMARY KEY, QUEUE_REF STRING, ARCHIVE_JSON VARIANT, ARCHIVED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3430_signature_entitlement_audit_trail.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ENTITLEMENT_AUDIT_TRAIL ( AUDIT_ID STRING PRIMARY KEY, TENANT_ID STRING, FEATURE_NAME STRING, ACTION STRING, PERFORMED_BY STRING, PERFORMED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), DETAILS VARIANT ); @31 @24

