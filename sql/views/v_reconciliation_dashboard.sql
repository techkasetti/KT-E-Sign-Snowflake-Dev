Purpose: consolidated reconciliation dashboard view (invoices, evidence manifests, OCSP status, alerts) used by Admin dashboard and SLO checks; supports operator workflows. @118 @214

-- v_reconciliation_dashboard.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE VIEW DOCGEN.V_RECONCILIATION_DASHBOARD AS
SELECT r.RECON_ID, r.MANIFEST_ID, r.EXPECTED_ROWS, r.ACTUAL_ROWS, r.STATUS, r.CHECKED_AT,
(SELECT COUNT(*) FROM DOCGEN.ALERTS a WHERE a.PAYLOAD:manifest_id::STRING = r.MANIFEST_ID AND a.RESOLVED = FALSE) AS ACTIVE_ALERTS,
(SELECT COUNT(*) FROM DOCGEN.EVIDENCE_BUNDLE eb WHERE eb.ARCHIVE_LOCATION IS NULL) AS MISSING_ARCHIVES,
(SELECT COUNT(*) FROM DOCGEN.PKI_OCSP_STATUS p WHERE p.STATUS = 'REVOKED') AS REVOKED_CERTS
FROM DOCGEN.EVIDENCE_RECONCILIATION r
ORDER BY r.CHECKED_AT DESC;

----
Operational note sentence citing design: these additional artifacts implement reconcile, retention purge with audit, HSM/External Function verification adapter, scheduled tasks and admin views and are consistent with the evidence/PKI, External Function, task scheduling and DR/runbook patterns described in your Snowflake design materials and runbooks. @214 @79 @29

Continuing will emit the remaining tranche (streamlined bulk registration driver, packaged ZIP structure and a full example GitHub Actions workflow that runs register_all_tranches.sh and the CI tests) next; these complete the E‑Signature Snowflake module operational and compliance coverage per your repo guidance. @31 @109

- User:  

- Anara: Continuing with more complete, copy/paste‑ready Snowflake artifacts for the E‑Signature module (export manifest, OCSP poller, Slack notifier, external‑function registration, tasks, registers and tests) — all produced to follow the Snowpark/PUT→CREATE PROCEDURE registration, External Function/API_INTEGRATION, S3 snapshot/export, OCSP/HSM and operational task patterns in your design docs. @62 @6

