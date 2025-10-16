Purpose: register the reconcile_evidence_exports proc following the PUT/CREATE pattern used in your Snowpark runbooks. @29 @51

#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/reconcile_evidence_exports.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.RECONCILE_EVIDENCE_EXPORTS(manifest_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/reconcile_evidence_exports.py') HANDLER='reconcile_evidence_exports';"
echo "RECONCILE_EVIDENCE_EXPORTS registered."

----
