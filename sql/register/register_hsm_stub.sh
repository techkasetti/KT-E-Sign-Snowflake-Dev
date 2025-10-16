Purpose: Register the HSM signer stub stored-proc for dev flow; replace with actual HSM integration for production. @96 @70
#!/usr/bin/env bash
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://sql/procs/hsm_signer_stub.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.HSM_SIGN_STUB(request_id STRING, signer_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/hsm_signer_stub.py') HANDLER='hsm_signer_stub';"
echo "HSM signer stub registered."

