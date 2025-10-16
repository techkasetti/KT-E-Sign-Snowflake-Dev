-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.SCHEDULE_PROVIDER_BACKOFF_07(backoff_id STRING, provider_name STRING, next_retry_at TIMESTAMP_LTZ, attempts INT, last_error VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.PROVIDER_BACKOFF_07 (BACKOFF_ID, PROVIDER_NAME, NEXT_RETRY_AT, ATTEMPTS, LAST_ERROR) VALUES (:backoff_id, :provider_name, :next_retry_at, :attempts, :last_error);
$$

