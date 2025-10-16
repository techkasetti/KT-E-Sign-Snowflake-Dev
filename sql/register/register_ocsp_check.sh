#!/usr/bin/env bash
set -e
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "PUT file://sql/procs/ocsp_probe.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.OCSP_PROBE(fingerprint STRING) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/ocsp_probe.py') HANDLER='ocsp_probe';"
echo "OCSP_PROBE registered."

Registration order and script structure align with the standard registration pattern in your runbooks @31 @36. @31 @36

