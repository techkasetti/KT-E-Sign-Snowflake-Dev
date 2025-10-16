CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_WEBHOOK_FILTERS(provider STRING, raw_payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/apply_webhook_filters.py
$$;

