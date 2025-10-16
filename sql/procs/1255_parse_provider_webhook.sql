-- Generic parser that uses WEBHOOK_SCHEMA_REGISTRY mapping to transform provider payloads into canonical SIGNATURE_EVENTS. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.PARSE_PROVIDER_WEBHOOK(provider STRING, raw_payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/parse_provider_webhook.py
$$;

