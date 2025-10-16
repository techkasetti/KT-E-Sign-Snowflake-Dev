USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
-- Smoke test verifying ingest -> verify -> evidence -> billing preview
INSERT INTO DOCGEN.SIGNATURE_EVENTS_RAW (RAW_ID, SOURCE_SYSTEM, PAYLOAD) VALUES (UUID_STRING(), 'test', PARSE_JSON('{"event_id":"evt_test_1","request_id":"req_test_1","document_id":"doc_test_1","signer_id":"signer_1","account_id":"acct_test","event_type":"SIGNED"}'));
CALL DOCGEN.PROCESS_RAW_SIGNATURE_EVENTS(10);
CALL DOCGEN.VERIFY_SIGNATURE_EXTENDED('req_test_1');
CALL DOCGEN.WRITE_EVIDENCE_BUNDLE(PARSE_JSON('{"request_id":"req_test_1","document_id":"doc_test_1","account_id":"acct_test","bundle_url":"s3://demo/bundles/b1","manifest":{}}'));
SELECT * FROM DOCGEN.SIGNATURE_VERIFICATIONS WHERE REQUEST_ID = 'req_test_1';
-- Smoke test follows acceptance flow described in runbooks @36 @62

