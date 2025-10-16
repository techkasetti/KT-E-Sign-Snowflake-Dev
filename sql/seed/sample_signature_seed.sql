-- Sample seed data for E‑Signature module (demo)
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

INSERT INTO DOCGEN.ACCOUNTS (ACCOUNT_ID, ACCOUNT_NAME) VALUES ('acct_demo_1','Demo Account 1');
INSERT INTO DOCGEN.SIGNERS (SIGNER_ID, ACCOUNT_ID, SIGNER_NAME, SIGNER_EMAIL, SIGNER_ROLE) VALUES ('s_demo_1','acct_demo_1','Alice Example','alice@example.com','signer');
INSERT INTO DOCGEN.TEMPLATES (TEMPLATE_ID, ACCOUNT_ID, TEMPLATE_NAME, TEMPLATE_BODY) VALUES ('tpl_demo_1','acct_demo_1','Demo Template','<html><body><h1>Demo</h1></body></html>');
INSERT INTO DOCGEN.SIGNATURE_REQUESTS (REQUEST_ID, DOCUMENT_ID, ACCOUNT_ID, TEMPLATE_ID, STATUS, CREATED_BY) VALUES ('req_demo_1','doc_demo_1','acct_demo_1','tpl_demo_1','SENT','system');

-- Example raw event row (to exercise Snowpipe ingestion)
INSERT INTO DOCGEN.SIGNATURE_EVENTS_RAW (RAW_VARIANT, FILE_NAME) SELECT PARSE_JSON('{"event_id":"evt_demo_1","request_id":"req_demo_1","signer_id":"s_demo_1","event_type":"VIEWED","ts":"2025-01-01T12:00:00Z","device":{"type":"browser"},"ip":"203.0.113.10","ua":"demo-agent/1.0"}'), 'seed_demo.json';

-- Seed a certificate (dev)
INSERT INTO DOCGEN.PKI_CERTIFICATE_STORE (CERT_ID, CERT_PEM, SUBJECT, ISSUER, SERIAL_NUMBER, FINGERPRINT, NOT_BEFORE, NOT_AFTER) VALUES ('cert_demo_1','-----BEGIN CERT-----\nMIID...demo\n-----END CERT-----','CN=Demo Signer','CN=Demo CA','123456','fp_demo_1',CURRENT_TIMESTAMP(),DATEADD('year',1,CURRENT_TIMESTAMP()));

-- End of seed set (used by CI smoke tests and staging). @31 @291

