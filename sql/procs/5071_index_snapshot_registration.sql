PUT file://sql/procs/register_index_snapshot.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.REGISTER_INDEX_SNAPSHOT(snapshot_manifest VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/register_index_snapshot.py')
HANDLER = 'register_index_snapshot';

Registers index snapshot manifests and triggers FAISS builder orchestration. @66 @31

