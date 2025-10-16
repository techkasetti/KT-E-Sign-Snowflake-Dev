-- Registration driver SQL to create or replace procedures (used after snowsql PUT). Follows deployment pattern in runbook. @29 @31
-- Example: run in CI after PUT of Python files to @~/
CREATE OR REPLACE PROCEDURE DOCGEN.DUMMY_REGISTER() RETURNS STRING LANGUAGE SQL AS $$ SELECT 'ok' $$;

