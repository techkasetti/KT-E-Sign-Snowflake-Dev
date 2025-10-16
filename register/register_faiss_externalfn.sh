#!/usr/bin/env bash
# Register FAISS external function helper (concrete account and role used as emitted)
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/external_functions/register_faiss_externalfn.sql
echo "FAISS External Function registered."

