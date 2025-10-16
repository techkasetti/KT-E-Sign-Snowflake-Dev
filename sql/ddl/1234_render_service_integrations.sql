-- Registry of external render services and credentials (do not store secrets in table). @31 @24 @52
CREATE OR REPLACE TABLE DOCGEN.RENDER_SERVICE_REGISTRY (
  SERVICE_ID STRING PRIMARY KEY,
  SERVICE_NAME STRING,
  ENDPOINT STRING,
  CONFIG VARIANT
);

