CREATE OR REPLACE PROCEDURE DOCGEN.TRIGGER_FAISS_BUILD(snapshot_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='trigger_faiss_build';

Triggers orchestration for FAISS index build after snapshot creation. @66 @31

