CREATE OR REPLACE PROCEDURE DOCGEN.ISSUE_PHONE_CODE(signer_id STRING, phone STRING, ttl_minutes INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/issue_phone_code.py
$$;

