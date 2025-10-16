-- Deterministic invoice hash UDF (SQL) that computes a canonical hash from line items for billing determinism
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

CREATE OR REPLACE FUNCTION DOCGEN.CANONICAL_INVOICE_HASH(line_items VARIANT)
RETURNS STRING
LANGUAGE SQL
AS $$
  SELECT MD5(ARRAY_TO_STRING(ARRAY_AGG(CONCAT(item:description::string, '|', TO_CHAR(item:units::number), '|', TO_CHAR(item:unit_price::number))) ORDER BY 1, ',')) FROM (SELECT VALUE AS item FROM TABLE(FLATTEN(INPUT => :line_items)))
$$;

