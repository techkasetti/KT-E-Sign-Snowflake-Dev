#!/usr/bin/env bash
# Register reconcile_invoices stored-proc
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/reconcile_invoices.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.RECONCILE_INVOICES(billing_run_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/reconcile_invoices.py') HANDLER='reconcile_invoices';"
echo "RECONCILE_INVOICES registered." @31 @59

