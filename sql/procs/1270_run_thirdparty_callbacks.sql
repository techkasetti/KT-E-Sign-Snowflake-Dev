-- Runner to deliver queued callbacks; uses External Function or middleware for HTTP delivery. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_THIRD_PARTY_CALLBACKS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_thirdparty_callbacks.py
$$;

