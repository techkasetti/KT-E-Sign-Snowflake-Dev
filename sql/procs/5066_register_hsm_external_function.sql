USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
-- External Function template for HSM signing/verification delegation
CREATE OR REPLACE API INTEGRATION DOCGEN.HSM_API_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/HSM_ROLE'
API_ALLOWED_PREFIXES = ('https://hsm.example.com/');
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.HSM_SIGN(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = DOCGEN.HSM_API_INTEGRATION
HEADERS = (('Authorization','<<AUTH_PLACEHOLDER>>'));

External Function pattern for delegating signing/verifications to HSM or managed PKI. @56 @31

