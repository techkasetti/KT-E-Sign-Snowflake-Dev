CREATE OR REPLACE PROCEDURE DOCGEN.EVALUATE_PROVIDER_SLA(provider_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/evaluate_provider_sla.py
$$;

