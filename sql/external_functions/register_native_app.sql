Purpose: Register an API_INTEGRATION and example External Function to demonstrate Snowflake Native App / admin gateway wiring. @11 @36

CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.ADMIN_API_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/admin-api-role'
ALLOWED_PREFIXES = ('https://admin-api.prod.example.com/');

CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.ADMIN_VALIDATE_INTEGRATION_KEY(req VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.ADMIN_API_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
CONTEXT_HEADERS = ('Authorization')
AS 'https://admin-api.prod.example.com/v1/validate-key';

