-- Stored-proc worker to enqueue/send notifications via External Function alerter. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.SEND_NOTIFICATION_WORKER(notif_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('snowflake-snowpark-python')
HANDLER='handler'
AS
$$
# handler staged at @~/procedures/send_notification_worker.py
$$;

