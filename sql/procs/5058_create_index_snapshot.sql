PUT file://sql/procs/create_index_snapshot.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_INDEX_SNAPSHOT(snapshot_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/create_index_snapshot.py')
HANDLER = 'create_index_snapshot';

Registration pattern for FAISS snapshot manifest creation and S3 export orchestration. @31 @66

