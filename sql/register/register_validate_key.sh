#!/usr/bin/env bash
set -e
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "PUT file://sql/procs/validate_integration_key.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.VALIDATE_INTEGRATION_KEY(acct_id STRING, key_plain STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/validate_integration_key.py') HANDLER='validate_integration_key';"
echo "VALIDATE_INTEGRATION_KEY registered."

This registration is consistent with your native key-generation and validation patterns and security runbooks @31 @21. @31 @21

