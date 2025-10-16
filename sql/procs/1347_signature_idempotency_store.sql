-- Table to store idempotency keys for webhook dedupe as per ingestion idempotency patterns @1 @6.
CREATE OR REPLACE TABLE DOCGEN.IDEMPOTENCY_KEYS ( KEY_ID STRING PRIMARY KEY, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

