-- Provider heartbeat tracking table that operators can use to detect outages and trigger failover @1 @6.
CREATE OR REPLACE TABLE DOCGEN.PROVIDER_HEARTBEAT ( PROVIDER_ID STRING PRIMARY KEY, LAST_HEARTBEAT TIMESTAMP_LTZ, STATUS STRING, DETAILS VARIANT );

