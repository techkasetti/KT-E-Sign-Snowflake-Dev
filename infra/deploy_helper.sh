# Small deploy helper to run PUT + register SQL sequence; intended for CI runners as per runbook guidance. @29 @31
#!/bin/bash
snowsql -q "PUT file://src/snowpark/* @~/procedures/"
snowsql -f sql/procs/640_register_all_procs.sql
./ci/register_external_functions.sh

