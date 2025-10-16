-- Generate a one-time token for document download and persist expiry metadata. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.GENERATE_DOWNLOAD_TOKEN(docstore_id STRING, ttl_minutes INT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNATURE_DOWNLOAD_TOKENS (TOKEN_ID, DOCSTORE_ID, EXPIRES_AT) VALUES (UUID_STRING(), :docstore_id, DATEADD(minute, :ttl_minutes, CURRENT_TIMESTAMP()));
$$;

