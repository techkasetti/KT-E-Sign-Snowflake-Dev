Purpose: support Admin dashboard with aggregated evidence counts and missing artifacts per tenant and time window; follows telemetry & KPI patterns in your design docs. @70 @162
-- v_evidence_metrics.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE VIEW DOCGEN.V_EVIDENCE_METRICS AS
SELECT ACCOUNT_ID, COUNT(*) AS total_bundles,
SUM(CASE WHEN ARCHIVE_LOCATION IS NULL THEN 1 ELSE 0 END) AS missing_archives,
MIN(CREATED_AT) AS oldest_bundle,
MAX(CREATED_AT) AS newest_bundle
FROM DOCGEN.EVIDENCE_BUNDLE
GROUP BY ACCOUNT_ID;

