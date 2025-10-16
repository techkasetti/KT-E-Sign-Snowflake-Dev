#!/usr/bin/env bash
# Register newly added procs for export/purge/analytics/hsm
set -e
export SNOW_ACCOUNT="demo_account"
export SNOW_USER="sysadmin"
export SNOW_ROLE="SYSADMIN"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://snowpark/procedures/*.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_EVIDENCE_TO_S3(bucket_prefix STRING, days_back INT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/export_evidence_to_s3.py') HANDLER='export_evidence_to_s3';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.PURGE_OLD_DOCUMENTS(retention_days INT, dry_run BOOLEAN) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/purge_old_documents.py') HANDLER='purge_old_documents';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.RUN_SIGNATURE_ANALYTICS() RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/run_signature_analytics.py') HANDLER='run_signature_analytics';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.HSM_SIGN_VIA_EXTERNAL(request_id STRING, signer_id STRING, payload VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/hsm_sign_via_externalfn.py') HANDLER='hsm_sign_via_externalfn';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.REGISTER_INDEX_SNAPSHOT(snapshot_id STRING, index_name STRING, s3_prefix STRING, shard_count NUMBER, index_version STRING, checksum STRING) RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/register_index_snapshot.py') HANDLER='register_index_snapshot';"
echo "Additional demo procedures registered."

