USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_EMBEDDINGS_TO_S3(snapshot_id STRING, s3_path STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
-- Placeholder COPY INTO using stage storage integration
INSERT INTO DOCGEN.INDEX_SNAPSHOTS (SNAPSHOT_ID, CREATED_BY, MANIFEST, LOCATION, CREATED_AT)
VALUES (snapshot_id, CURRENT_USER(), PARSE_JSON('{}'), s3_path, CURRENT_TIMESTAMP());
RETURN PARSE_JSON('{"status":"export_scheduled","snapshot_id":' || QUOTE_LITERAL(snapshot_id) || '}');
$$;

Schedules embedding export to object storage for FAISS builder to pick up. @31 @66

