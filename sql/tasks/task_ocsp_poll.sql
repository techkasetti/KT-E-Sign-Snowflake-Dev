Purpose: scheduled OCSP poller task to call CHECK_OCSP_AND_UPDATE on cadence per PKI runbook; follows task scheduling guidance. @176
-- task_ocsp_poll.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TASK DOCGEN.TASK_OCSP_POLL
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 */6 * * * UTC' -- every 6 hours
AS CALL DOCGEN.CHECK_OCSP_AND_UPDATE(100);

