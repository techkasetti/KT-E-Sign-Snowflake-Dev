-- Backfill processor that replays events to rebuild indices/provenance @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.PROCESS_EVENTS_BACKFILL()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/process_events_backfill.py
$$;

