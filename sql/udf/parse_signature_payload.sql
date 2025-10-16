-- SQL UDF to extract canonical signer identity from payload variant (example concrete extraction logic) @314
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE FUNCTION DOCGEN.PARSE_SIGNER_EMAIL(payload VARIANT)
RETURNS STRING
LANGUAGE SQL
AS $$
  SELECT COALESCE(payload:signer_email::string, payload:auth_payload:email::string, 'unknown@example.com');
$$;

