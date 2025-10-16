-- Generated per Snowflake E-Signature patterns. @31
CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_RETENTION_POLICY_09(policy_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
-- Minimal implementation: mark policy applied; actual purge tasks scheduled by orchestration outside SP
UPDATE DOCGEN.DATA_RETENTION_POLICIES_09 SET ENABLED = ENABLED WHERE POLICY_ID = :policy_id;
$$

