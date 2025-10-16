#!/usr/bin/env bash
# Registration driver — PUT python procs to @~/procedures and CREATE PROCEDURE
# Ensure SNOW_ACCOUNT, SNOW_USER, SNOW_ROLE are set in env or CI secrets

set -e
: "${SNOW_ACCOUNT:?Need SNOW_ACCOUNT env var}"
: "${SNOW_USER:?Need SNOW_USER env var}"
: "${SNOW_ROLE:?Need SNOW_ROLE env var}"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://snowpark/procedures/*.py @~/procedures/ AUTO_COMPRESS=TRUE;"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.GENERATE_INTEGRATION_KEY(account_id STRING) RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/generate_integration_key.py') HANDLER='generate_integration_key';"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.ENTITLEMENT_CHECK(account_id STRING, feature_key STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/entitlement_check.py') HANDLER='entitlement_check';"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.EMBEDDING_INGEST(staged_path STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/embedding_ingest_sp.py') HANDLER='embedding_ingest';"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.USAGE_INGEST(staged_path STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/usage_ingest_sp.py') HANDLER='usage_ingest';"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.PREVIEW_BILLING(account_id STRING, period_start STRING, period_end STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/preview_billing.py') HANDLER='preview_billing';"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BILLING_RUN(account_id STRING, period_start STRING, period_end STRING, preview_hash STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/run_billing_run.py') HANDLER='run_billing_run';"

# Signature procedures
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_SIGNATURE_WEBHOOK(payload VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/upsert_signature_webhook.py') HANDLER='upsert_signature_webhook';"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.VERIFY_SIGNATURE(request_id STRING, signer_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/verify_signature.py') HANDLER='verify_signature';"

snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_EVIDENCE_BUNDLE(payload VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/write_evidence_bundle.py') HANDLER='write_evidence_bundle';"

echo "Registration complete. Next: run external function registration and create pipes/tasks as needed."

