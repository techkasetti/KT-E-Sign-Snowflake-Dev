CREATE OR REPLACE PROCEDURE DOCGEN.NOTIFY_SUBSCRIBERS(alert_type STRING, alert_details VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/notify_subscribers.py
$$;

