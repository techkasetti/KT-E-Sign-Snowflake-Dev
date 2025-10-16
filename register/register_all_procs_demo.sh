#!/usr/bin/env bash
# Demo variant of registration driver that uses demo env variables defined in deploy_demo_env.sh
set -e
export SNOW_ACCOUNT=${SNOW_ACCOUNT:-demo_account}
export SNOW_USER=${SNOW_USER:-sysadmin}
export SNOW_ROLE=${SNOW_ROLE:-SYSADMIN}
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://snowpark/procedures/*.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.GENERATE_INTEGRATION_KEY(account_id STRING) RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/generate_integration_key.py') HANDLER='generate_integration_key';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.ENTITLEMENT_CHECK(account_id STRING, feature_key STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/entitlement_check.py') HANDLER='entitlement_check';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.EMBEDDING_INGEST(staged_path STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/embedding_ingest_sp.py') HANDLER='embedding_ingest';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.USAGE_INGEST(staged_path STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/usage_ingest_sp.py') HANDLER='usage_ingest';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.PREVIEW_BILLING(account_id STRING, period_start STRING, period_end STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/preview_billing.py') HANDLER='preview_billing';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BILLING_RUN(account_id STRING, period_start STRING, period_end STRING, preview_hash STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/run_billing_run.py') HANDLER='run_billing_run';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_SIGNATURE_WEBHOOK(payload VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/upsert_signature_webhook.py') HANDLER='upsert_signature_webhook';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.VERIFY_SIGNATURE(request_id STRING, signer_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/verify_signature.py') HANDLER='verify_signature';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_EVIDENCE_BUNDLE(payload VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/write_evidence_bundle.py') HANDLER='write_evidence_bundle';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.RUN_SIGNATURE_ANALYTICS() RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/run_signature_analytics.py') HANDLER='run_signature_analytics';"
echo "Demo procedure registration finished."

