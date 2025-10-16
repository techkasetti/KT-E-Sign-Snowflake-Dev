#!/usr/bin/env bash
# Register compute_bundle_hash procedure (used by evidence zipper)
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/compute_bundle_hash.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.COMPUTE_BUNDLE_HASH(manifest VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/compute_bundle_hash.py') HANDLER='compute_bundle_hash';"
echo "COMPUTE_BUNDLE_HASH registered."

