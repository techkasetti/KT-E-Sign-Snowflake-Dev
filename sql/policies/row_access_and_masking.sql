Purpose: example Row Access Policy and Masking Policy for tenant isolation and PII masking per your governance recommendations @316 @346.  
-- row_access_and_masking.sql  
CREATE OR REPLACE MASKING POLICY DOCGEN.MASK_EMAIL_AS_PII (val STRING) RETURNS STRING -> CASE WHEN CURRENT_ROLE() IN ('DOCGEN_ADMIN','DOCGEN_MONITOR') THEN val ELSE regexp_replace(val, '@.*$', '@***') END;  
CREATE OR REPLACE ROW ACCESS POLICY DOCGEN.ROW_ACCESS_BY_ACCOUNT (allowed_account STRING) RETURNS BOOLEAN -> CURRENT_ACCOUNT() IS NOT NULL AND allowed_account = CURRENT_ROLE();  
ALTER TABLE DOCGEN.SIGNERS MODIFY COLUMN EMAIL SET MASKING POLICY DOCGEN.MASK_EMAIL_AS_PII;  

