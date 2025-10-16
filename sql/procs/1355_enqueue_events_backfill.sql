-- Add a request to backfill queue for reprocessing events and rebuilding provenance @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.ENQUEUE_EVENTS_BACKFILL(request_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.EVENTS_BACKFILL_QUEUE (BACKFILL_ID, REQUEST_ID) VALUES (UUID_STRING(), :request_id);
$$;

