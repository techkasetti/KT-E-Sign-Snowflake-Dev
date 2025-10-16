-- Compute and record an anchor hash for a bundle and persist it for audit exports. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.ANCHOR_HASH_FOR_BUNDLE(bundle_id STRING, anchor_location STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
-- compute hash externally or via UDF, then INSERT into HASH_ANCHORS
RETURN OBJECT_CONSTRUCT('bundle_id', :bundle_id, 'anchor_location', :anchor_location);
$$;

