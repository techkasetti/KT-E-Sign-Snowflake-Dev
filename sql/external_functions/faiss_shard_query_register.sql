Purpose: template SQL to register a per-shard FAISS query external function that calls a containerized per-shard query service; use your API gateway and API_INTEGRATION patterns as described in the External Function guidance @31 @4.
-- faiss_shard_query_register.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.FAISS_QUERY_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/faiss-query-role'
ALLOWED_PREFIXES = ('https://faiss-query.example.com/');
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.FAISS_SHARD_QUERY(shard_id STRING, payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.FAISS_QUERY_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
AS 'https://faiss-query.example.com/shard/{shard_id}/search'; @31 @4

