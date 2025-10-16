-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_INTEGRATION_HEALTH_07(health_id STRING, integration_name STRING, status STRING, last_check TIMESTAMP_LTZ, details VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.INTEGRATION_HEALTH_07 (HEALTH_ID, INTEGRATION_NAME, STATUS, LAST_CHECK, DETAILS) VALUES (:health_id, :integration_name, :status, :last_check, :details);
$$

