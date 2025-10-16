CREATE OR REPLACE PROCEDURE DOCGEN.RUN_PROVIDER_COMPLIANCE_CHECK(provider_id STRING, check_name STRING, result VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.PROVIDER_COMPLIANCE_CHECKS (CHECK_ID, PROVIDER_ID, CHECK_NAME, RESULT) VALUES (UUID_STRING(), :provider_id, :check_name, :result);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2820_signature_service_endpoints.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SERVICE_ENDPOINTS (
  ENDPOINT_ID STRING PRIMARY KEY,
  NAME STRING,
  TYPE STRING,
  URL STRING,
  META VARIANT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

