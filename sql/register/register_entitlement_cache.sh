#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/update_entitlement_cache.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.UPDATE_ENTITLEMENT_CACHE() RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/update_entitlement_cache.py') HANDLER='update_entitlement_cache';"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/tasks/task_update_entitlement.sql
echo "Entitlement cache proc and task registered." @31 @11

