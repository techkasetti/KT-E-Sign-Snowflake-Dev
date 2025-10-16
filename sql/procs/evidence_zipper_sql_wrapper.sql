Purpose: SQL wrapper to call evidence zipper and return manifest for use by Admin UI flows. @70 @113
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.EVIDENCE_ZIPPER_SQL(request_id STRING, created_by STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ CALL DOCGEN.EVIDENCE_ZIPPER(request_id, created_by); $$;

