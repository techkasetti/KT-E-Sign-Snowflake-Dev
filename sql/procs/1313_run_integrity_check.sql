-- Runner that verifies checksums and anchors for a given bundle id; invokes reconcilers and alerts on mismatch. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_INTEGRITY_CHECK(bundle_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_integrity_check.py
$$;

