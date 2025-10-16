-- External Function for provider healthcheck endpoint registration used by monitoring tasks @1 @6.
CREATE OR REPLACE API_INTEGRATION docgen_health_integration API_PROVIDER = aws_api_gateway API_AWS_ROLE_ARN = 'arn:aws:iam::REPLACE_ME:role/SnowflakeApiRole' ENABLED = TRUE;
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.PROVIDER_HEALTH_CHECK(payload VARIANT) RETURNS VARIANT API_INTEGRATION = docgen_health_integration HEADERS = ( 'Content-Type' = 'application/json' );

