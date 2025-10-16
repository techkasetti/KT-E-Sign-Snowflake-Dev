-- Example row access policy template (apply per sensitive table)
CREATE OR REPLACE ROW ACCESS POLICY DOCGEN.ROW_ACCESS_ACCOUNT_POLICY AS (CURRENT_ROLE() IN ('DOCGEN_ADMIN') OR CURRENT_ROLE() IN ('DOCGEN_MONITOR'));

Row access/masking policies follow the least-privilege and row-level access design guidelines in your security docs @158 @18. @158 @18

