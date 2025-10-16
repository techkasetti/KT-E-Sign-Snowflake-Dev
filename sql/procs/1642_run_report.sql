CREATE OR REPLACE PROCEDURE DOCGEN.RUN_REPORT(report_id STRING, run_by STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_report.py
$$;

