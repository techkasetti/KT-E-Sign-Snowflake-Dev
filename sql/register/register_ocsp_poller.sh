Purpose: register check_ocsp_and_update proc and OCSP task with PUT → CREATE → TASK pattern as recommended in runbooks. @31 @176
#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/check_ocsp_and_update.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.CHECK_OCSP_AND_UPDATE(limit NUMBER) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/check_ocsp_and_update.py') HANDLER='check_ocsp_and_update';"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/tasks/task_ocsp_poll.sql
echo "OCSP poller proc and task registered."

