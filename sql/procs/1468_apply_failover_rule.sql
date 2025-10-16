CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_FAILOVER_RULE(rule_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
-- operator-defined logic to switch providers based on RULE_ID
RETURN OBJECT_CONSTRUCT('rule_id', :rule_id, 'status', 'applied');
$$;

