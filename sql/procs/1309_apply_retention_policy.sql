-- Apply retention policy to configured target tables (invoked by scheduler). @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_RETENTION_POLICY(policy_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
-- operator will implement table-specific purge logic based on policy
RETURN OBJECT_CONSTRUCT('policy_id', :policy_id, 'status','applied');
$$;

