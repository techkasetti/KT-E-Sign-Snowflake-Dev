-- Select and apply escalation policy for an alert event to notify Ops/Owners. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_ESCALATION_POLICY(alert_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/apply_escalation_policy.py
$$;

