-- External Function / API_INTEGRATION registration templates (replace placeholders)
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.AGENT_INTEGRATION
  ENABLED = TRUE
  API_AWS_ROLE_ARN = 'arn:aws:iam::REPLACE:role/api_integration_role'
  ALLOWED_PREFIXES = ('https://your-agent-gateway.example.com/');

CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.SIMILARITY_QUERY(req VARIANT)
  RETURNS VARIANT
  API_INTEGRATION = AI_FEATURE_HUB.AGENT_INTEGRATION
  HEADERS = ('Content-Type'='application/json')
  CONTEXT_HEADERS = ('Authorization')
  AS 'https://your-agent-gateway.example.com/similarity';

-- Note: register signature provider gateways or use middleware to receive webhooks and call the stored procedures.

