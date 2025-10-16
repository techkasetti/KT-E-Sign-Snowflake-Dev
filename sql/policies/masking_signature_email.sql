-- Example masking policy to hide signer emails from non-admin roles as recommended. @31 @76
CREATE OR REPLACE MASKING POLICY mask_signer_email AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DOCGEN_SIGNATURE_ADMIN') THEN val
    ELSE '****@****'
  END;
ALTER TABLE DOCGEN.SIGNERS MODIFY COLUMN SIGNER_EMAIL SET MASKING POLICY mask_signer_email;

