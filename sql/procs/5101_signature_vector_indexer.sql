PUT file://sql/procs/signature_vector_indexer.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.SIGNATURE_VECTOR_INDEXER(limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/signature_vector_indexer.py')
HANDLER = 'signature_vector_indexer';

Indexer that writes embeddings into SIGNATURE_VECTOR_STORE and prepares index snapshot manifests. @100 @31

