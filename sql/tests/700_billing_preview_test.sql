-- Deterministic billing preview test: assert invoice_hash stability per preview handler design. @49 @24
CALL DOCGEN.PREVIEW_BILLING('acct-1', CURRENT_TIMESTAMP());
-- Use the returned invoice_hash in verification harness to assert deterministic behavior.

