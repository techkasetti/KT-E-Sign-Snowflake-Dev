-- Sequence emulation table for deterministic IDs in demo/test (Snowflake uses UUID functions but we provide helper)
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SEQ_IDS ( SEQ_NAME STRING PRIMARY KEY, LAST_VAL NUMBER );
INSERT INTO DOCGEN.SEQ_IDS VALUES ('request_seq',0);

