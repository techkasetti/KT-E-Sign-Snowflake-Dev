-- Record a ratecard event for billing pipelines to consume during preview/commit. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_RATECARD_EVENT(account_id STRING, template_id STRING, event_type STRING, event_value VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNATURE_RATECARD_EVENTS (EVENT_ID, ACCOUNT_ID, TEMPLATE_ID, EVENT_TYPE, EVENT_VALUE) VALUES (UUID_STRING(), :account_id, :template_id, :event_type, :event_value);
$$;

