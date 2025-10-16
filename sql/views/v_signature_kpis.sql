Purpose: KPI view summarizing signing throughput, latencies, success rates and top signers for admin dashboards and SLO monitoring; follows telemetry and monitoring artifacts in your spec @281 @326.
-- v_signature_kpis.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE VIEW DOCGEN.V_SIGNATURE_KPIS AS
SELECT
    DATE_TRUNC('hour', EVENT_TS) AS hour,
    COUNT(*) FILTER (WHERE EVENT_TYPE = 'VIEWED') AS views,
    COUNT(*) FILTER (WHERE EVENT_TYPE = 'SIGNED') AS signed,
    COUNT(*) FILTER (WHERE EVENT_TYPE = 'REJECTED') AS rejected,
    AVG(DATEDIFF('second', MIN(EVENT_TS) OVER (PARTITION BY REQUEST_ID), MAX(EVENT_TS) OVER (PARTITION BY REQUEST_ID))) AS avg_signing_duration,
    (COUNT(*) FILTER (WHERE EVENT_TYPE = 'SIGNED')::FLOAT / NULLIF(COUNT(*) FILTER (WHERE EVENT_TYPE IN ('VIEWED','SIGNED','REJECTED')),0)) AS success_rate
FROM DOCGEN.SIGNATURE_EVENTS
GROUP BY 1
ORDER BY 1 DESC; @281 @326

