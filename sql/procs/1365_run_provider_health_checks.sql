-- Runner that invokes provider health External Function and updates DOCGEN.SIGNATURE_PROVIDER_STATUS @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_PROVIDER_HEALTH_CHECKS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_provider_health_checks.py
$$;

