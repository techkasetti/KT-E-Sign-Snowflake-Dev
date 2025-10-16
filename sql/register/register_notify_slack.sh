Purpose: register notify_slack proc and the SLACK external function registration in CI order. @31 @62
#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/external_functions/register_slack_externalfn.sql
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/notify_slack.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.NOTIFY_SLACK(alert_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/notify_slack.py') HANDLER='notify_slack';"
echo "NOTIFY_SLACK registered and SLACK external function ensured."

