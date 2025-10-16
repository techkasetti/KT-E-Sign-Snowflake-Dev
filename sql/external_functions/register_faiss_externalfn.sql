-- Register FAISS similarity External Function and API_INTEGRATION (production-style demo) @63 @4
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.FAISS_QUERY_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/faiss-query-role'
ALLOWED_PREFIXES = ('https://faiss-query.prod.example.com/');

CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.FAISS_SIMILARITY(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.FAISS_QUERY_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
CONTEXT_HEADERS = ('Authorization')
AS 'https://faiss-query.prod.example.com/v1/similarity';

