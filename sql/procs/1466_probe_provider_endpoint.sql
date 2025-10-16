CREATE OR REPLACE PROCEDURE DOCGEN.PROBE_PROVIDER_ENDPOINT(endpoint_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/probe_provider_endpoint.py
$$;

