CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_ALERT_RULE_02(rule_id STRING, name STRING, severity STRING, condition VARIANT, actions VARIANT, enabled BOOLEAN)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
MERGE INTO DOCGEN.ALERT_RULES_02 t
USING (SELECT :rule_id AS rid, :name AS nm, :severity AS sev, :condition AS cond, :actions AS acts, :enabled AS en) s
ON t.RULE_ID = s.rid
WHEN MATCHED THEN UPDATE SET NAME = s.nm, SEVERITY = s.sev, CONDITION = s.cond, ACTIONS = s.acts, ENABLED = s.en
WHEN NOT MATCHED THEN INSERT (RULE_ID, NAME, SEVERITY, CONDITION, ACTIONS, ENABLED) VALUES (s.rid, s.nm, s.sev, s.cond, s.acts, s.en);
$$

