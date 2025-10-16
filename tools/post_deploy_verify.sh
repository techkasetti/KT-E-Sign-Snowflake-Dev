#!/usr/bin/env bash
# Quick verification checks
set -e
SNOW_ACC="client_prod_001"
SNOW_USER="svc_docgen"
SNOW_ROLE="DOCGEN_ADMIN"
echo "Checking signature requests count..."
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "SELECT COUNT(*) FROM DOCGEN.SIGNATURE_REQUESTS;"
echo "Checking pending alerts..."
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -q "SELECT COUNT(*) FROM DOCGEN.ALERTS WHERE RESOLVED = FALSE;"
echo "Smoke test: call smoke runner"
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/tests/smoke_run_all.sql

