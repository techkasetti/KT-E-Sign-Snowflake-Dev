USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TASK DOCGEN.TASK_ENFORCE_RETENTION
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 3 * * * UTC'
AS CALL DOCGEN.PURGE_OLD_EVIDENCE(365);

Scheduling retention purge follows the operational runbook sequence for scheduled maintenance and retention enforcement in your docs @75 @36. @75 @36

