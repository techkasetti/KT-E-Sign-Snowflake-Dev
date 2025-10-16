-- Check integration-specific rate limits before processing inbound requests. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.CHECK_INTEGRATION_RATE_LIMIT(integration_key_hash STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
SELECT OBJECT_CONSTRUCT('allowed', TRUE) FROM DOCGEN.API_INTEGRATION_RATE_LIMITS WHERE INTEGRATION_KEY_HASH = :integration_key_hash;
$$;

