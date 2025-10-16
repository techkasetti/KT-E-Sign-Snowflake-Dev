CREATE OR REPLACE PROCEDURE DOCGEN.VERIFY_EXPORT(bundle_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='verify_export';

Verifies exported evidence bundles post-copy to object storage and writes verification results. @36 @31

