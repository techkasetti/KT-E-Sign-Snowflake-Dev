Purpose: register alert detector procedure and task using snowsql in CI fallback pattern @9 @106.  
#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/alert_detector.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.ALERT_DETECTOR(max_missing NUMBER) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/alert_detector.py') HANDLER='alert_detector';"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -f sql/tasks/task_alert_detector.sql
echo "ALERT_DETECTOR registered and task created."  

