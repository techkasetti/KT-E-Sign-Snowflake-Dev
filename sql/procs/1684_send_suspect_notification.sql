CREATE OR REPLACE PROCEDURE DOCGEN.SEND_SUSPECT_NOTIFICATION(suspect_id STRING, payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/send_suspect_notification.py
$$;

