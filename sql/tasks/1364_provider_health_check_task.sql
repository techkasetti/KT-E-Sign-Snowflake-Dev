-- Task to periodically call provider health EF and update provider status table @1 @6.
CREATE OR REPLACE TASK DOCGEN.TASK_PROVIDER_HEALTH WAREHOUSE = WH_PROC SCHEDULE = 'USING CRON */5 * * * * UTC' AS CALL DOCGEN.RUN_PROVIDER_HEALTH_CHECKS();

