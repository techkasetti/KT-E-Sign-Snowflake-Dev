#!/usr/bin/env bash
# Registration script: PUT python files to user stage then CREATE/REPLACE PROCEDURE with IMPORTS.
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"

snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://snowpark/procedures/*.py @~/procedures/ AUTO_COMPRESS=TRUE;"

snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_SIGNATURE_REQUEST(account_id STRING, document_id STRING, template_id STRING, requester_id STRING, signers VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/create_signature_request.py') HANDLER='create_signature_request';"

snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.INGEST_SIGNATURE_EVENTS() RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/ingest_signature_events.py') HANDLER='ingest_signature_events';"

snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_SIGNATURE_WEBHOOK(raw_event VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/upsert_signature_webhook.py') HANDLER='upsert_signature_webhook';"

snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.VALIDATE_SIGNATURE_AND_RECORD(request_id STRING, signer_id STRING, signature_b64 STRING, cert_chain VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/validate_signature_and_record.py') HANDLER='validate_signature_and_record';"

snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.RUN_SIGNATURE_ANALYTICS() RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/run_signature_analytics.py') HANDLER='run_signature_analytics';"

echo "Signature procedures registered."

