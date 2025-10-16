CREATE OR REPLACE FUNCTION DOCGEN.UUID_STRING()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$ return require('crypto').randomBytes(16).toString('hex'); $$;

