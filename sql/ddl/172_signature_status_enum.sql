-- Utility supporting table for allowed statuses and their semantics (used by orchestration). @31 @11
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_STATUS_LU (
  STATUS_KEY STRING PRIMARY KEY,
  DESCRIPTION STRING
);
INSERT INTO DOCGEN.SIGNATURE_STATUS_LU (STATUS_KEY, DESCRIPTION) VALUES
('PENDING','Awaiting action'),
('SIGNED','Signed by signer'),
('COMPLETED','All signers completed'),
('REVOKED','Signature revoked');

