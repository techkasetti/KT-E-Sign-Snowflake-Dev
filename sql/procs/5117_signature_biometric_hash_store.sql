CREATE OR REPLACE PROCEDURE DOCGEN.STORE_BIOMETRIC_HASH(signer_id STRING, hash_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNER_THREAT_INTEL (INTEL_ID, SIGNER_ID, SOURCE, RISK_LEVEL, DETAILS, REPORTED_AT)
VALUES (UUID_STRING(), signer_id, 'biometric_hash', 'LOW', PARSE_JSON('{"hash":'||QUOTE_LITERAL(hash_value)||'}'), CURRENT_TIMESTAMP());
$$;

Stores privacy-preserving biometric template hashes as indexed intel records. @336 @31

