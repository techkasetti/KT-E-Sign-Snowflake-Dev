USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ENDUSER_CONSENT_AUDIT ( AUDIT_ID STRING PRIMARY KEY, CONSENT_ID STRING, ACTION STRING, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @1 @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3001_signature_compliance_contact_directory.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.COMPLIANCE_CONTACT_DIRECTORY ( CONTACT_ID STRING PRIMARY KEY, TENANT_ID STRING, NAME STRING, EMAIL STRING, PHONE STRING, ROLE STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

