-- Register VERIFY_SIGNATURE stored procedure wrapper invoking Snowpark handler; follows PUT/import pattern described in your runbook. @29 @31
CREATE OR REPLACE PROCEDURE DOCGEN.VERIFY_SIGNATURE(request_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'handler'
AS
$$
# handler implementation is staged at @~/procedures/verify_signature.py
$$;

