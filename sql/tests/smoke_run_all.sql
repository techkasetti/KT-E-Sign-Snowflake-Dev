-- Smoke runner: sequence to validate end-to-end flows (dev)
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

-- 1) Ingest pipe will pick up files if uploaded; otherwise call ingestion manually
CALL DOCGEN.INGEST_SIGNATURE_EVENTS();

-- 2) Upsert demo webhook
CALL DOCGEN.UPSERT_SIGNATURE_WEBHOOK(PARSE_JSON('{"event_id":"evt_smoke_1","request_id":"req_demo_1","signer_id":"s_demo_1","event_type":"SIGNED","ts":"2024-01-01T12:00:00Z","device":{"device":"browser"},"ip":"1.2.3.4","ua":"smoke-agent"}'));

-- 3) Validate signature recorded
SELECT * FROM DOCGEN.SIGNATURE_VALIDATION WHERE REQUEST_ID='req_demo_1' AND SIGNER_ID='s_demo_1' ORDER BY VERIFIED_AT DESC LIMIT 5;

-- 4) Trigger evidence zipper
CALL DOCGEN.EVIDENCE_ZIPPER('req_demo_1','smoke-run');

-- 5) Run analytics
CALL DOCGEN.RUN_SIGNATURE_ANALYTICS();

-- 6) Check alerts
SELECT * FROM DOCGEN.ALERTS ORDER BY ALERT_TS DESC LIMIT 20;

