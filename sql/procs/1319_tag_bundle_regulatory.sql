-- Attach regulatory tags to evidence bundles to guide export/retention workflows. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.TAG_BUNDLE_REGULATORY(bundle_id STRING, jurisdiction STRING, regulation STRING, notes STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.REGULATORY_TAGS (TAG_ID, BUNDLE_ID, JURISDICTION, REGULATION, NOTES) VALUES (UUID_STRING(), :bundle_id, :jurisdiction, :regulation, :notes);
$$;

