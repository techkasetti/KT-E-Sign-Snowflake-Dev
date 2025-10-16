#!/usr/bin/env bash
# Register verify_signature_extended procedure
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://snowpark/procedures/verify_signature_extended.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.VERIFY_SIGNATURE_EXTENDED(validation_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/verify_signature_extended.py') HANDLER='verify_signature_extended';"
echo "VERIFY_SIGNATURE_EXTENDED registered."

