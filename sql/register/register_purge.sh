#!/usr/bin/env bash
set -e
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "PUT file://sql/procs/purge_old_evidence.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.PURGE_OLD_EVIDENCE(days INT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/purge_old_evidence.py') HANDLER='purge_old_evidence';"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r DOCGEN_ADMIN -f sql/tasks/task_enforce_retention.sql
echo "PURGE_OLD_EVIDENCE and TASK_ENFORCE_RETENTION registered and scheduled."

This registration script and task resume pattern matches the CI/deploy runbook guidance for scheduled tasks and retention operations @36 @16. @36 @16

