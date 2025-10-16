USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.SIMILARITY_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/similarity-caller'
ALLOWED_PREFIXES = ('https://faiss-query.example.com/');
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.SIMILARITY_QUERY(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.SIMILARITY_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
AS 'https://faiss-query.example.com/query';

This External Function registration matches the External Function/API_INTEGRATION registration pattern and secure gateway guidance in your Snowflake materials @33 @57. @33 @57

