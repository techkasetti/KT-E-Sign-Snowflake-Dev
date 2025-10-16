Purpose: register the generate and validate integration key procedures using SNOWPUT→CREATE approach as required by Snowpark registration patterns @9 @29.  
#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/generate_integration_key.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.GENERATE_INTEGRATION_KEY(acct_id STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/generate_integration_key.py') HANDLER='generate_integration_key';"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/validate_integration_key.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.VALIDATE_INTEGRATION_KEY(acct_id STRING, plaintext STRING, salt STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/validate_integration_key.py') HANDLER='validate_integration_key';"
echo "Integration key procs registered."  

