Purpose: Stored-proc registration SQL helpers to support index append/flush and manifest updates; used in FAISS snapshot orchestration. @6 @22

USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

-- Example helper: registration of trigger proc to record snapshot manifest entry (proc body is Snowpark Python). 
-- Register a Snowpark procedure that the FAISS builder calls back to record the manifest entry. 
CREATE OR REPLACE PROCEDURE DOCGEN.REGISTER_INDEX_SNAPSHOT(snapshot_id STRING, index_name STRING, s3_prefix STRING, shard_count NUMBER, index_version STRING, checksum STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION='3.10'
  HANDLER='register_index_snapshot'
  IMPORTS = ('@~/procedures/register_index_snapshot.py');

