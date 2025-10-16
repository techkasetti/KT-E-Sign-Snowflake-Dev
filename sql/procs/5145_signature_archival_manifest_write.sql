CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_ARCHIVAL_MANIFEST(bundle_id STRING, manifest VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='write_archival_manifest';

Writes archival manifest entries linking evidence bundles to S3 export artifacts. @36 @31

