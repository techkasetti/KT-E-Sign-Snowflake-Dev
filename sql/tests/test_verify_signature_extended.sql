-- SQL test harness: simulate verification call and expected result
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

-- Simulate insertion of a validation row
INSERT INTO DOCGEN.SIGNATURE_VALIDATION (VALIDATION_ID, REQUEST_ID, SIGNER_ID, VALIDATION_STATUS, VALIDATION_HASH, VERIFIED_AT)
VALUES ('val_test_1','req_demo_1','s_demo_1','SIGNED_BY_HSM','hash_val_1',CURRENT_TIMESTAMP());

-- Call verification
CALL DOCGEN.VERIFY_SIGNATURE('req_demo_1','s_demo_1');

-- Query result expected to be verified true (manual assert via CI script)

