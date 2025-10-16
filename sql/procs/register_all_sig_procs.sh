#!/usr/bin/env bash
set -e
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/register/register_upsert_signature_webhook.sql
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/register/register_verify_signature_extended.sql
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/register/register_write_evidence_bundle.sql
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/external_functions/register_ocsp_and_hsm.sql
snowsql -a $SNOW_ACC -u $SNOW_USER -r $SNOW_ROLE -f sql/external_functions/register_faiss_service.sql
# Registration driver follows bundle registration/runbook pattern @31 @36

