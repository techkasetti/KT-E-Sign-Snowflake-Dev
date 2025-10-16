-- Execute configured audit exports and write manifests to specified stage @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_AUDIT_EXPORTS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_audit_exports.py
$$;

