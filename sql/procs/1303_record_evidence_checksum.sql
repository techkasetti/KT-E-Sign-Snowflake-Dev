-- Record checksum for a given file in a bundle to support later verification. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_EVIDENCE_CHECKSUM(bundle_id STRING, filename STRING, checksum STRING, algo STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.EVIDENCE_CHECKSUMS (CHECKSUM_ID, BUNDLE_ID, FILENAME, CHECKSUM, ALGO) VALUES (UUID_STRING(), :bundle_id, :filename, :checksum, :algo);
$$;

