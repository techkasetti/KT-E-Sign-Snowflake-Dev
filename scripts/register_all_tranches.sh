Purpose: consolidated driver that runs all registration scripts in dependency order (PUT → CREATE PROCEDURE / CREATE TASK / CREATE EXTERNAL FUNCTION) to deploy the full tranche; follow the PUT/CREATE pattern in your runbooks @35 @55.
#!/usr/bin/env bash
set -e
# Example order: DDL -> Grants -> PUT Python files -> register procs -> create tasks -> resume tasks
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/ddl/signature_domain_schema.sql
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/grants/create_roles_and_grants.sql
./sql/register/register_write_evidence.sh
./sql/register/register_verify_signature.sh
./sql/register/register_alert_detector.sh
./sql/register/register_reconcile_proc.sh
./sql/register/register_retention_proc.sh
./sql/register/register_export_manifest.sh
./sql/register/register_ocsp_check.sh
./sql/register/register_reconcile_invoices.sh
# External functions
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/external_functions/register_slack_externalfn.sql
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/external_functions/register_ocsp_externalfn.sql
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/external_functions/faiss_shard_query_register.sql
# Resume tasks (safe to run in staging)
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "ALTER TASK DOCGEN.TASK_ALERT_DETECTOR RESUME; ALTER TASK DOCGEN.TASK_NOTIFY_SLACK RESUME; ALTER TASK DOCGEN.TASK_ENFORCE_RETENTION RESUME; ALTER TASK DOCGEN.TASK_OCSP_POLL RESUME; ALTER TASK DOCGEN.TASK_UPDATE_ENTITLEMENT RESUME;"
echo "All tranches registered. Validate via smoke tests and runbooks." @35 @55

