-- Record a lineage event when ingesting or exporting signature-related artifacts. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_LINEAGE_EVENT(src_system STRING, src_id STRING, tgt_table STRING, tgt_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.DATA_LINEAGE_EVENTS (LINEAGE_ID, SOURCE_SYSTEM, SOURCE_ID, TARGET_TABLE, TARGET_ID) VALUES (UUID_STRING(), :src_system, :src_id, :tgt_table, :tgt_id);
$$;

