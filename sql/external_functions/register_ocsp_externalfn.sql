Purpose: External Function registration template for OCSP probe service (HSM/OCSP gateway) following the External Function/API_INTEGRATION patterns in your design docs. @112 @176
-- register_ocsp_externalfn.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.OCSP_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/ocsp-probe-role'
ALLOWED_PREFIXES = ('https://ocsp-probe.example.com/');
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.OCSP_CHECK(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.OCSP_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
AS 'https://ocsp-probe.example.com/check';

