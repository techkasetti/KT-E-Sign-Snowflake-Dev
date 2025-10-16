CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_ACCOUNT_EVIDENCE(account_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='export_account_evidence';

Exports all evidence bundles for an account to archival S3 location for compliance. @36 @31

