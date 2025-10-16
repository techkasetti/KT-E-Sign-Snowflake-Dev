-- Helper to record a signer's consent text and timestamp. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_CONSENT(request_id STRING, signer_id STRING, consent_text CLOB)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNATURE_CONSENTS (CONSENT_ID, REQUEST_ID, SIGNER_ID, CONSENT_TEXT) VALUES (UUID_STRING(), :request_id, :signer_id, :consent_text);
$$;

