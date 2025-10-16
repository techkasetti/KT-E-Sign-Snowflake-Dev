-- Scheduler task to process retry queue and invoke stored-procs as needed. @31 @24 @52
CREATE OR REPLACE TASK DOCGEN.TASK_PROCESS_RETRY_QUEUE WAREHOUSE = WH_PROC SCHEDULE = 'USING CRON */5 * * * * UTC' AS CALL DOCGEN.PROCESS_RETRY_QUEUE();

