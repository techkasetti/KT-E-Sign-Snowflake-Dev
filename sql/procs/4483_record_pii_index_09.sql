-- Generated per Snowflake E-Signature patterns. @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_PII_INDEX_09(pii_id STRING, table_name STRING, column_name STRING, sensitivity STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.PII_INDEX_09 (PII_ID, TABLE_NAME, COLUMN_NAME, SENSITIVITY) VALUES (:pii_id, :table_name, :column_name, :sensitivity);
$$

