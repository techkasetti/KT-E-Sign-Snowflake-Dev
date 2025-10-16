Purpose: register an External Function pointing at an HSM-backed signer/verification gateway per your HSM/PKI integration guidance. @79 @214

-- register_hsm_signer.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.HSM_SIGNER_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/hsm-signer-role'
ALLOWED_PREFIXES = ('https://hsm-signer.example.com/');
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.HSM_SIGNER_VERIFY(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.HSM_SIGNER_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
AS 'https://hsm-signer.example.com/verify';

----
