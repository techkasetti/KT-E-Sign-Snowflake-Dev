USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CUSTOMER_NOTIFICATION_PREFERENCES ( PREF_ID STRING PRIMARY KEY, TENANT_ID STRING, PREF_JSON VARIANT, UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1951_signature_audit_retention_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.AUDIT_RETENTION_INDEX ( IDX_ID STRING PRIMARY KEY, AUDIT_REF STRING, RETENTION_UNTIL TIMESTAMP_LTZ, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24 @1
