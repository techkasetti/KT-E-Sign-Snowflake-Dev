USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_AUDIT_EXPORT_LOGS ( LOG_ID STRING PRIMARY KEY, EXPORT_ID STRING, TENANT_ID STRING, STATUS STRING, STARTED_AT TIMESTAMP_LTZ, COMPLETED_AT TIMESTAMP_LTZ );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3701_signature_policy_audit_snapshots.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_AUDIT_SNAPSHOTS ( SNAP_ID STRING PRIMARY KEY, POLICY_ID STRING, SNAP_JSON VARIANT, TAKEN_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @1
