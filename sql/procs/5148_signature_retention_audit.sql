CREATE OR REPLACE PROCEDURE DOCGEN.RUN_RETENTION_AUDIT()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='run_retention_audit';

Audit procedure to validate purge completeness and retention policy adherence. @36 @31

