-- Lookup for acceptable event types and semantics used by ingestion logic. @31 @24 @52
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_EVENT_TYPES (
  EVENT_TYPE STRING PRIMARY KEY,
  DESCRIPTION STRING
);
INSERT INTO DOCGEN.SIGNATURE_EVENT_TYPES (EVENT_TYPE, DESCRIPTION) VALUES
('VIEWED','Signer viewed document'),
('SIGNED','Signer completed signature'),
('DECLINED','Signer declined');

