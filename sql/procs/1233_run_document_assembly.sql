-- Document assembly worker proc which may call external renderer service (External Function) and updates job status. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_DOCUMENT_ASSEMBLY()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_document_assembly.py
$$;

