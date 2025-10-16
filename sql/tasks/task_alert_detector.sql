Purpose: schedule the alert detector task at operational cadence as described in the operator runbooks and CI smoke tests @41 @216.  
-- task_alert_detector.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TASK DOCGEN.TASK_ALERT_DETECTOR WAREHOUSE = 'COMPUTE_WH' SCHEDULE = 'USING CRON */10 * * * * UTC' AS CALL DOCGEN.ALERT_DETECTOR(10);  

