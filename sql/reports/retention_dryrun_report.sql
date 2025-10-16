Purpose: Produce a report of what would be purged by retention enforcement (dry-run) without deleting rows — useful for compliance review prior to purge. @241
-- retention_dryrun_report.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
SELECT BUNDLE_ID, ARCHIVE_LOCATION, CREATED_AT, METADATA:legal_hold::BOOLEAN AS legal_hold
FROM DOCGEN.EVIDENCE_BUNDLE
WHERE CREATED_AT <= DATEADD('day', -1095, CURRENT_TIMESTAMP())
AND COALESCE(METADATA:legal_hold::BOOLEAN, FALSE) = FALSE
ORDER BY CREATED_AT
LIMIT 1000;

