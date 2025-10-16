-- Raw staging table for signature events from Snowpipe before idempotent MERGE into SIGNATURE_EVENTS as per staging pattern. @14 @31
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_EVENTS_RAW (
  RAW_JSON VARIANT,
  LOADED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

