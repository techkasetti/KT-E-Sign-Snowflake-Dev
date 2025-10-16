-- Evaluate alert policies and emit alerts when thresholds breach using EMIT_ALERT proc @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.EVALUATE_ALERT_POLICIES()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/evaluate_alert_policies.py
$$;

