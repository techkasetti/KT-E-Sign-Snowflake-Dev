CREATE OR REPLACE TASK DOCGEN.TASK_APPLY_POLICIES
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
CALL DOCGEN.SYNC_ADMIN_POLICIES();

Daily policy applicator task to bring per-account policies up to date. @37 @31

