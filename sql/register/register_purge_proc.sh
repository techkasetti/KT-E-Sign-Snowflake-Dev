Purpose: register purge procedure and create scheduled retention enforcement task pattern documented in runbooks. @118 @214

#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/purge_evidence_with_audit.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.PURGE_EVIDENCE_WITH_AUDIT(cutoff_days NUMBER, dry_run BOOLEAN) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/purge_evidence_with_audit.py') HANDLER='purge_evidence_with_audit';"
# retention task (weekly)
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE TASK DOCGEN.TASK_ENFORCE_RETENTION WAREHOUSE = 'COMPUTE_WH' SCHEDULE = 'USING CRON 0 5 * * 0 UTC' AS CALL DOCGEN.PURGE_EVIDENCE_WITH_AUDIT(1095, TRUE);"
echo "PURGE_EVIDENCE_WITH_AUDIT registered and retention task created (dry-run mode)."

----
