CREATE OR REPLACE PROCEDURE DOCGEN.CHECK_SLA_AND_EMIT()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/check_sla_and_emit_alerts.py
$$;

