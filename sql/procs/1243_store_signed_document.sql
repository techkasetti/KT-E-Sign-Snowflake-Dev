-- Store reference to signed document after assembly/signing. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.STORE_SIGNED_DOCUMENT(request_id STRING, signed_url STRING, storage_meta VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.SIGNED_DOCUMENT_STORE (DOCSTORE_ID, REQUEST_ID, SIGNED_URL, STORAGE_META) VALUES (UUID_STRING(), :request_id, :signed_url, :storage_meta); $$;

