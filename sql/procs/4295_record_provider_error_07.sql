-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_PROVIDER_ERROR_07(error_id STRING, provider_name STRING, bundle_id STRING, error_text STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.PROVIDER_ERRORS_07 (ERROR_ID, PROVIDER_NAME, BUNDLE_ID, ERROR_TEXT) VALUES (:error_id, :provider_name, :bundle_id, :error_text);
$$

