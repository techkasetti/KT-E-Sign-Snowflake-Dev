-- Runner that looks up enabled exports and triggers EXPORT_EVIDENCE_TO_S3 for matching requests. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BATCH_EXPORTS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_batch_exports.py
$$;

