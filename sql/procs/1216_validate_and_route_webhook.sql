-- Validation and routing proc: verifies signature webhooks HMAC/Idempotency and routes to UPSERT_SIGNATURE_WEBHOOK. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.VALIDATE_AND_ROUTE_WEBHOOK(raw_payload VARIANT, signature_header STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('snowflake-snowpark-python')
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/validate_and_route_webhook.py
$$;

