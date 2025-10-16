# Quickstart runbook (staging)
1) Seed sample data: snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/seed/sample_signature_seed.sql. @31
2) Create stage & pipe: snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/snowpipe/create_stage_and_pipe.sql. @39
3) Register stored-procs & external functions: ./sql/register/register_all_remaining.sh (this script PUTs Python procs and registers procedures). @92
4) Start tasks: snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "ALTER TASK DOCGEN.TASK_INGEST_SIGNATURE_EVENTS RESUME; ALTER TASK DOCGEN.TASK_ALERT_DISPATCH RESUME; ALTER TASK DOCGEN.TASK_ALERT_DETECTOR RESUME;" @31
5) Run smoke tests: pytest -q tests/test_ingest_and_alerts.py and pytest -q tests/test_e2e_evidence_flow.py. @69

- Operational notes: replace any real credentials in CI with secrets manager values; do not commit keys. @65

