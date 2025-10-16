Purpose: register the export_evidence_to_s3 proc using PUT → CREATE PROCEDURE as required by Snowpark registration runbooks. @31 @62
#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/export_evidence_to_s3.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_EVIDENCE_TO_S3(s3_bucket STRING, s3_prefix STRING, since_days INT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/export_evidence_to_s3.py') HANDLER='export_evidence_to_s3';"
echo "EXPORT_EVIDENCE_TO_S3 registered."

