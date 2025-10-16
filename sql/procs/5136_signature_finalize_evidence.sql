CREATE OR REPLACE PROCEDURE DOCGEN.FINALIZE_EVIDENCE(bundle_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='finalize_evidence';

Finalization procedure that ensures evidence bundling includes certificates, OCSP snapshots, and anchor hashes. @36 @31

