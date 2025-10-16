USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.OCSP_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/ocsp-role'
ALLOWED_PREFIXES = ('https://ocsp-gateway.example.com/');
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.OCSP_CHECK(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.OCSP_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
AS 'https://ocsp-gateway.example.com/check';

This External Function registration secures OCSP gateway calls via API_INTEGRATION per your security and external-function patterns @33 @72. @33 @72

