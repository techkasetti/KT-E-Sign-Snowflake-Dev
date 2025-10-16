-- Minimal README with exact registration/verification steps (PUT -> CREATE/REPLACE -> REGISTER EF -> RESUME TASKS) per runbook guidance. @29 @31
-- 1) PUT Python files: snowsql -q "PUT file://src/snowpark/* @~/procedures/"
-- 2) Run sql/procs/*_register.sql with snowsql to CREATE/REPLACE PROCEDURE
-- 3) Run ci/register_external_functions.sh to register API_INTEGRATION and EXTERNAL FUNCTION
-- 4) Seed sample data and run tests/tests/smoke/*.sql

