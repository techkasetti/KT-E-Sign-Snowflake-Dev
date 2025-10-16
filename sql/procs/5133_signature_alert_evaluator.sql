CREATE OR REPLACE PROCEDURE DOCGEN.EVALUATE_ALERT_RULES()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='evaluate_alert_rules';

Evaluates alert rules against telemetry/health and enqueues notifications. @63 @31

