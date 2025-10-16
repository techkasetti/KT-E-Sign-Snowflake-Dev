Purpose: Register assemble_document stored-proc for Admin UI integration. @31 @28
#!/usr/bin/env bash
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "PUT file://sql/procs/assemble_document.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "CREATE OR REPLACE PROCEDURE DOCGEN.ASSEMBLE_DOCUMENT(request_id STRING, template_id STRING, clauses VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/assemble_document.py') HANDLER='assemble_document';"
echo "ASSEMBLE_DOCUMENT registered."

