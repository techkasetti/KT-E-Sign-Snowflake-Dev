-- External Function template to call an OCSP/validation service; fill API_INTEGRATION values in your environment before registering per security guidance. @23 @14
CREATE OR REPLACE API_INTEGRATION docgen_ocsp_integration
  API_PROVIDER = aws_api_gateway
  API_AWS_ROLE_ARN = 'arn:aws:iam::REPLACE_ME:role/SnowflakeApiRole'
  ENABLED = TRUE;
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.OCSP_CHECK(payload VARIANT)
  RETURNS VARIANT
  API_INTEGRATION = docgen_ocsp_integration
  HEADERS = ( 'Content-Type' = 'application/json' );

