-- PUT file://snowpark/procedures/write_evidence_bundle.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_EVIDENCE_BUNDLE(bundle_meta VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/write_evidence_bundle.py')
HANDLER = 'write_evidence_bundle';
-- Evidence assembly writer registration per runbook patterns @31 @362

