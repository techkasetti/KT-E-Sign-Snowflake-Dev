-- Add comments and clustering guidance (demo)
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

COMMENT ON TABLE DOCGEN.SIGNATURE_EVENTS IS 'Normalized signature events extracted from raw stage. Use STREAM DOCGEN.STREAM_SIG_EVENTS_RAW to process incoming files via Snowpipe.';
COMMENT ON TABLE DOCGEN.SIGNATURE_REQUESTS IS 'High-level signature request per document. Status flows: PENDING -> RENDERED -> COMPLETED.';
-- Example clustering recommendation
ALTER TABLE DOCGEN.SIGNATURE_EVENTS CLUSTER BY (REQUEST_ID, SIGNER_ID);

