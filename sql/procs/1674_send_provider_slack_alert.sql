CREATE OR REPLACE PROCEDURE DOCGEN.SEND_PROVIDER_SLACK_ALERT(provider_id STRING, alert_body VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged at @~/procedures/send_provider_slack_alert.py
$$;

