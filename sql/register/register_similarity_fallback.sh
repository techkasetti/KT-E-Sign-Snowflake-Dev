#!/usr/bin/env bash
set -e
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "PUT file://sql/procs/similarity_fallback_udf.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.SIMILARITY_FALLBACK(embedding VARIANT, top_k NUMBER) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/similarity_fallback_udf.py') HANDLER='similarity_fallback';"
echo "SIMILARITY_FALLBACK registered."

This registration script uses the PUT→CREATE PROCEDURE flow recommended in your Snowpark deployment runbook @31 @36. @31 @36

