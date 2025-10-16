-- Task scheduling ingestion from raw staging per Snowpipe → Task orchestration patterns. @14 @31
CREATE OR REPLACE TASK DOCGEN.TASK_INGEST_RAW
  WAREHOUSE = WH_PROC
  SCHEDULE = 'USING CRON * * * * * UTC'
AS
  CALL DOCGEN.INGEST_RAW_TO_EVENTS();

