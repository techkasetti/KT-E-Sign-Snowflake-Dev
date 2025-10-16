CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_DISPUTE_RESOLUTION_06(res_id STRING, dispute_id STRING, resolved_by STRING, resolution TEXT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.DISPUTE_RESOLUTIONS_06 (RES_ID, DISPUTE_ID, RESOLVED_BY, RESOLUTION) VALUES (:res_id, :dispute_id, :resolved_by, :resolution);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4100_signature_review_workflows_06.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.REVIEW_WORKFLOWS_06 (
  WORKFLOW_ID STRING PRIMARY KEY,
  BUNDLE_ID STRING,
  NAME STRING,
  STEPS VARIANT,
  OWNER STRING,
  STATUS STRING DEFAULT 'ENABLED',
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

