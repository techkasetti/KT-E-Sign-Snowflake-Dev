-- Example stored-proc that can call OCSP External Function for queued certs and update SIGNATURE_CERTIFICATES table with responses. @12 @68
CREATE OR REPLACE PROCEDURE DOCGEN.POLL_OCSP_FOR_CERTS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('snowflake-snowpark-python')
HANDLER='handler'
AS
$$
# handler staged at @~/procedures/poll_ocsp.py
$$;

