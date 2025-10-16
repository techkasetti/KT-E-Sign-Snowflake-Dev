#!/usr/bin/env bash
# Shell helper to call reconciliation stored-proc for latest manifest
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"
MANIFEST_ID="$1"
if [ -z "$MANIFEST_ID" ]; then
  echo "Usage: $0 <manifest_id>"
  exit 1
fi
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CALL DOCGEN.RECONCILE_EVIDENCE_EXPORTS('${MANIFEST_ID}');"

