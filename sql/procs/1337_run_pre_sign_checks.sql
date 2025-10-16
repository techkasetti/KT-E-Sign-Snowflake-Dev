-- Run configured pre-sign checks and persist outcomes before enabling signature actions. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_PRE_SIGN_CHECKS(request_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_pre_sign_checks.py
$$;

