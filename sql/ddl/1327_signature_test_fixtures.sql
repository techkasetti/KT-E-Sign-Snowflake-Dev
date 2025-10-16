-- Seed data fixtures used by smoke and CI tests for signature end-to-end flows. @1 @31
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
INSERT INTO DOCGEN.SIGNATURE_REQUESTS (REQUEST_ID, DOCUMENT_ID, ACCOUNT_ID, CREATED_BY, STATUS) VALUES ('req-0001','doc-0001','acct-1','system','PENDING');

