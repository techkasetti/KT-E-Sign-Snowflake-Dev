CREATE OR REPLACE PROCEDURE DOCGEN.ENQUEUE_ADMIN_AUDIT_EXPORT_09(export_id STRING, scope VARIANT, requested_by STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.ADMIN_AUDIT_EXPORTS_09 (EXPORT_ID, SCOPE, REQUESTED_BY, STATUS) VALUES (:export_id, :scope, :requested_by, 'QUEUED');
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4526_signature_verification_metadata_10.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.VERIFICATION_METADATA_10 (
  META_ID STRING PRIMARY KEY,
  BUNDLE_ID STRING,
  PROVIDER STRING,
  METHOD STRING,
  DETAILS VARIANT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

