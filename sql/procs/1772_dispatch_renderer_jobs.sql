CREATE OR REPLACE PROCEDURE DOCGEN.DISPATCH_RENDERER_JOBS(batch_size INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/dispatch_renderer_jobs.py
$$;

