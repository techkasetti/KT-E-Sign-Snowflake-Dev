-- Schedule an audit export job to copy audit artifacts to a secure stage for auditors. CREATE OR REPLACE PROCEDURE DOCGEN.SCHEDULE_AUDIT_EXPORT(schedule_cron STRING, target_stage STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.AUDIT_EXPORT_JOBS (JOB_ID, SCHEDULE_CRON, TARGET_STAGE, STATUS) VALUES (UUID_STRING(), :schedule_cron, :target_stage, 'ENABLED'); $$;

