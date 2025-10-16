PUT file://sql/procs/notification_worker.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.NOTIFICATION_WORKER(limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/notification_worker.py')
HANDLER = 'notification_worker';

Worker that dequeues NOTIFICATION_QUEUE items and calls External Function notifiers. @24 @31

