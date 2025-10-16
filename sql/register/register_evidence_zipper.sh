Purpose: Put and register the evidence zipper stored procedure as a Snowpark Python proc. @31 @70
#!/usr/bin/env bash
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://sql/procs/evidence_zipper.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.EVIDENCE_ZIPPER(request_id STRING, created_by STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/evidence_zipper.py') HANDLER='evidence_zipper';"
echo "EVIDENCE_ZIPPER registered."

