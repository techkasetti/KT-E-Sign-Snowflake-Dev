-- Simple SQL UDF to compute SHA256 invoice/document hashes used in billing/evidence workflows per deterministic hashing guidance. @49 @21
CREATE OR REPLACE FUNCTION DOCGEN.SHA256_HEX(inp STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var crypto = require('crypto');
  return crypto.createHash('sha256').update(inp).digest('hex');
$$;

