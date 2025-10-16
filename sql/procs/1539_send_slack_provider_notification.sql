CREATE OR REPLACE PROCEDURE DOCGEN.SEND_SLACK_PROVIDER_NOTIFICATION(provider_id STRING, payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/send_slack_provider_notification.py
$$;

