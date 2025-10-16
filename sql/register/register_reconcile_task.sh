Purpose: register and enable the reconcile evidence task; conforms with runbook ordering for tasks & procs. @29 @35

#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/register/register_reconcile_evidence.sh
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/tasks/task_reconcile_evidence.sql
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "ALTER TASK DOCGEN.TASK_RECONCILE_EVIDENCE RESUME;"

----
