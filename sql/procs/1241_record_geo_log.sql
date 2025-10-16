-- Record geo data alongside signature events for analytics and threat detection. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_GEO_LOG(event_id STRING, ip STRING, geo_info VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.SIGNATURE_GEO_LOG (GEO_ID, EVENT_ID, IP_ADDRESS, GEO_INFO) VALUES (UUID_STRING(), :event_id, :ip, :geo_info); $$;

