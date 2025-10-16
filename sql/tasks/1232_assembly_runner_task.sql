-- Task to run document assembly workers that render PDFs and store to stage. @31 @24 @52
CREATE OR REPLACE TASK DOCGEN.TASK_RUN_ASSEMBLY WAREHOUSE = WH_PROC SCHEDULE = 'USING CRON */1 * * * * UTC' AS CALL DOCGEN.RUN_DOCUMENT_ASSEMBLY();

