CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_FINAL_STATUS(bundle_id STRING, status STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
UPDATE DOCGEN.SIGNATURE_EVIDENCE_BUNDLE SET META = OBJECT_INSERT(META, 'final_status', status), CREATED_AT = CURRENT_TIMESTAMP() WHERE BUNDLE_ID = bundle_id;
$$;

Marks evidence bundles with final compliance status for export and retention. @36 @31

