#!/usr/bin/env bash
# Register the ingest_signature_events procedure and resume ingestion task
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://snowpark/procedures/ingest_signature_events.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.INGEST_SIGNATURE_EVENTS() RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/ingest_signature_events.py') HANDLER='ingest_signature_events';"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "ALTER TASK DOCGEN.TASK_INGEST_SIGNATURE_EVENTS RESUME;"
echo "Ingest proc registered and task resumed."
