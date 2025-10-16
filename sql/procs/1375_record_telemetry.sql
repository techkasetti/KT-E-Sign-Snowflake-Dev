-- Procedure to record telemetry events from various procs and external functions @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_TELEMETRY(source STRING, event_name STRING, payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.TELEMETRY_EVENTS (TELE_ID, SOURCE, EVENT_NAME, PAYLOAD) VALUES (UUID_STRING(), :source, :event_name, :payload); $$;

