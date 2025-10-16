#!/usr/bin/env bash
# Register notify_pending_alerts procedure
set -e
export SNOW_ACCOUNT="client_prod_001"
export SNOW_USER="svc_docgen"
export SNOW_ROLE="DOCGEN_ADMIN"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://snowpark/procedures/notify_pending_alerts.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.NOTIFY_PENDING_ALERTS(batch_size NUMBER) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/notify_pending_alerts.py') HANDLER='notify_pending_alerts';"
echo "Registered NOTIFY_PENDING_ALERTS."

