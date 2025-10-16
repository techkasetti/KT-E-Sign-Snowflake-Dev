#!/usr/bin/env bash
# Bulk register remaining procedures, tasks, and external functions for E-Signature module
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"

# Put Python procs
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://sql/procs/*.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/external_functions/register_ocsp_externalfn.sql
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/external_functions/register_email_alerter.sql
# Create procedures that rely on imports (examples)
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_EVIDENCE_TO_S3(manifest_id STRING, s3_prefix STRING, cut_off_days NUMBER) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/export_evidence_to_s3.py') HANDLER='export_evidence_to_s3';"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.PURGE_OLD_DOCUMENTS(retention_days NUMBER, dry_run BOOLEAN) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/purge_old_documents.py') HANDLER='purge_old_documents';"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.CHECK_OCSP_AND_UPDATE(cert_fingerprint STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/check_ocsp_and_update.py') HANDLER='check_ocsp_and_update';"

# Register maintenance SQL objects
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/ddl/pki_and_ocsp_schema.sql
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/policies/row_access_and_masking.sql

echo "All remaining modules registered."

This registration script bundles final registration steps for OCSP, email alerts, export and purge procedures and applies masking/row-access policies so the module can be enabled in one coordinated runbook @29 @101.

