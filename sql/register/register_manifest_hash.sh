#!/usr/bin/env bash
set -e
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "PUT file://sql/procs/compute_manifest_hash.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.COMPUTE_MANIFEST_HASH(manifest_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/compute_manifest_hash.py') HANDLER='compute_manifest_hash';"
echo "COMPUTE_MANIFEST_HASH registered."

This script registers the manifest hash procedure and ties into the evidence reconciliation CI steps referenced in your runbooks @23 @66. @23 @66

