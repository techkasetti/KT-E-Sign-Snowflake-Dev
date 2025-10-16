#!/usr/bin/env bash
# Quick script to spin up demo assets: run DDL, register procs, seed data, resume tasks
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/ddl/signature_domain_schema.sql
./register/register_signature_procs.sh
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/seed/sample_signature_seed.sql
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "ALTER TASK DOCGEN.TASK_INGEST_SIGNATURE_EVENTS RESUME; ALTER TASK DOCGEN.TASK_SIGNATURE_ANALYTICS RESUME;"
echo "Demo environment prepared."

