-- UDF to normalize email addresses for dedupe and matching in signer flows. @31 @24 @52
CREATE OR REPLACE FUNCTION DOCGEN.NORMALIZE_EMAIL(inp STRING) RETURNS STRING LANGUAGE SQL AS $$ LOWER(TRIM(inp)) $$;

