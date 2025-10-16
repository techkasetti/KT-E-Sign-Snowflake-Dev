PUT file://sql/procs/process_event_backfill.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.PROCESS_EVENT_BACKFILL(limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/process_event_backfill.py')
HANDLER = 'process_event_backfill';

Backfill worker to process historical events for analytics and reconciliation. @101 @31

