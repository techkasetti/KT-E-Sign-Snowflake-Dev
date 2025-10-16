CREATE OR REPLACE PROCEDURE DOCGEN.RUN_PROVIDER_CERT_CHECK(provider_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_provider_cert_check.py
$$;

