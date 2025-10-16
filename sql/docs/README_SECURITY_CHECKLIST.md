-- Security checklist excerpt to enforce before production: HSM integration, remove dev stubs, secrets in vault, masking policies applied per runbook. @52 @31
-- 1) Replace dev signer stubs with HSM provider integration
-- 2) Configure OCSP provider and register External Function for OCSP calls
-- 3) Ensure STORAGE_INTEGRATION and S3 buckets are set in CI secrets (do not embed)

