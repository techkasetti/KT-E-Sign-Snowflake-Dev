#!/usr/bin/env bash
# Register retention enforcement procedure
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/enforce_retention_policy.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.ENFORCE_RETENTION_POLICY(retention_days NUMBER) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/enforce_retention_policy.py') HANDLER='enforce_retention_policy';"
echo "ENFORCE_RETENTION_POLICY registered."

