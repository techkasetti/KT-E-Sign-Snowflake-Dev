# RUNBOOK: Deploy Signature Module
1) Set env vars: SNOW_ACCOUNT, SNOW_USER, SNOW_ROLE, STORAGE_INTEGRATION, S3_BUCKET, API_AWS_ROLE_ARN
2) Apply DDL: snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/ddl/*.sql
3) PUT Python files: snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -q "PUT file://snowpark/procedures/*.py @~/procedures/ AUTO_COMPRESS=TRUE;"
4) Register procs: ./sql/procs/register_all_procs.sh
5) Register External Functions: snowsql -f sql/external_functions/register_ocsp_and_hsm.sql
6) Resume tasks: ALTER TASK DOCGEN.TASK_INGEST_SIGNATURE_EVENTS RESUME;
7) Run tests: snowsql -f sql/tests/test_signature_flow.sql
-- Runbook steps align with PUT->CREATE->TASK registration patterns in your bundle @31 @44

