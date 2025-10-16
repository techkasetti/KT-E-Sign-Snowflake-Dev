-- External Function template to call a containerized FAISS similarity service per FAISS snapshot/container pattern. @31 @34
CREATE OR REPLACE API_INTEGRATION docgen_faiss_integration
  API_PROVIDER = aws_api_gateway
  API_AWS_ROLE_ARN = 'arn:aws:iam::REPLACE_ME:role/SnowflakeApiRole'
  ENABLED = TRUE;
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.SIMILARITY_QUERY(query VARIANT)
  RETURNS VARIANT
  API_INTEGRATION = docgen_faiss_integration
  HEADERS = ( 'Content-Type' = 'application/json' );

