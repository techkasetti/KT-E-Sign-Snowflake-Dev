CREATE OR REPLACE PROCEDURE DOCGEN.RUN_DLQ_RETRIES(batch_size INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_dlq_retries.py
$$;

