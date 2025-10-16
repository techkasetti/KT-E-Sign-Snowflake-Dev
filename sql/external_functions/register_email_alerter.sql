-- Register Email Alerter External Function (concrete API endpoint and integration pattern)
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.EMAIL_ALERT_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/email-sender-role'
ALLOWED_PREFIXES = ('https://email.prod.example.com/'); CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.EMAIL_ALERT(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.EMAIL_ALERT_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
CONTEXT_HEADERS = ('Authorization')
AS 'https://email.prod.example.com/v1/send';
