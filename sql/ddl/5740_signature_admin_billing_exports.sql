USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.BILLING_EXPORTS ( EXPORT_ID STRING PRIMARY KEY, ACCOUNT_ID STRING, LOCATION STRING, STATUS STRING, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/5741_signature_audit_checksums.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.AUDIT_CHECKSUMS ( CHECKSUM_ID STRING PRIMARY KEY, RESOURCE_REF STRING, CHECKSUM STRING, COMPUTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
-- @31 @24

