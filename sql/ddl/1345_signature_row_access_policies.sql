-- Row access policy template for tenant isolation applied to signature objects @1 @6.
CREATE OR REPLACE ROW ACCESS POLICY docgen_tenant_row_access AS (account_id STRING) RETURNS BOOLEAN -> CURRENT_ROLE() IN ('SYSADMIN','DOCGEN_SIGNATURE_ADMIN') OR account_id = CURRENT_SESSION():ACCOUNT_ID; ALTER TABLE DOCGEN.SIGNATURE_REQUESTS ADD ROW ACCESS POLICY docgen_tenant_row_access ON (ACCOUNT_ID);

