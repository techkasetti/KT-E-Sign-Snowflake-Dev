USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE FUNCTION DOCGEN.MATCH_SIGNATURE_HASH(doc_hash STRING, signature_hash STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
SELECT IFF(doc_hash = signature_hash, TRUE, FALSE);
$$;
-- Simple deterministic hash match UDF used in verification flows @31 @24

