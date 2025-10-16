-- Optional biometrics metadata table for storing hashed biometric templates and trust scores per design notes (ensure privacy controls). @31 @431
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_BIOMETRICS (
  BIO_ID STRING PRIMARY KEY,
  SIGNER_ID STRING,
  BIOMETRIC_HASH STRING,
  TRUST_SCORE FLOAT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

