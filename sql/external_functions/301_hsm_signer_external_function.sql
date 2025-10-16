-- External Function template to call HSM-backed signing service (dev stubs must be replaced with HSM provider integration before QES). @52 @305
CREATE OR REPLACE API_INTEGRATION docgen_hsm_integration
  API_PROVIDER = aws_api_gateway
  API_AWS_ROLE_ARN = 'arn:aws:iam::REPLACE_ME:role/SnowflakeApiRole'
  ENABLED = TRUE;
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.HSM_SIGN(payload VARIANT)
  RETURNS VARIANT
  API_INTEGRATION = docgen_hsm_integration
  HEADERS = ( 'Content-Type' = 'application/json' );

