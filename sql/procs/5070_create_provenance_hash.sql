USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_PROVENANCE_HASH(bundle_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
-- compute deterministic provenance hash (example)
SELECT MD5(ARRAY_TO_STRING(ARRAY_AGG(COALESCE(TO_VARCHAR(DOCUMENT_ID),'')),',')) FROM DOCGEN.SIGNATURE_EVENTS WHERE BUNDLE_ID = bundle_id;
$$;

Deterministic provenance hash computation used for anchors and reconciliation. @41 @31

