-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_PROVIDER_METRIC_07(metric_id STRING, provider_name STRING, metric_key STRING, metric_value VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.PROVIDER_METRICS_07 (METRIC_ID, PROVIDER_NAME, METRIC_KEY, METRIC_VALUE) VALUES (:metric_id, :provider_name, :metric_key, :metric_value);
$$

