CREATE OR REPLACE PROCEDURE DOCGEN.DISPATCH_PENDING_NOTIFICATIONS(batch_size INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/dispatch_pending_notifications.py
$$;

