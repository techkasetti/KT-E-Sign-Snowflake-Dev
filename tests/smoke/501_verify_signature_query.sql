-- Smoke test to validate VERIFY_SIGNATURE inserted a verification row; intended for CI smoke harness. @27 @36
SELECT COUNT(*) AS cnt FROM DOCGEN.SIGNATURE_VERIFICATIONS WHERE REQUEST_ID = 'req-0001';

