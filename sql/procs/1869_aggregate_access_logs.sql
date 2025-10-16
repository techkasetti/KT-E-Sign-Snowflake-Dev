CREATE OR REPLACE PROCEDURE DOCGEN.AGGREGATE_ACCESS_LOGS(window_hours INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler @~/procedures/aggregate_access_logs.py
$$;

