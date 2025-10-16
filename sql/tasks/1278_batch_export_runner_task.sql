-- Task runner to trigger enabled batch exports per schedule table. @31 @24 @52
CREATE OR REPLACE TASK DOCGEN.TASK_RUN_BATCH_EXPORTS WAREHOUSE = WH_PROC SCHEDULE = 'USING CRON 0 * * * * UTC' AS CALL DOCGEN.RUN_BATCH_EXPORTS();

