CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_KPI_ALERT(alert_id STRING, kpi_id STRING, condition VARIANT, severity STRING, enabled BOOLEAN)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
MERGE INTO DOCGEN.KPI_ALERTS t USING (SELECT :alert_id AS aid, :kpi_id AS kid, :condition AS cond, :severity AS sev, :enabled AS en) s ON t.ALERT_ID = s.aid WHEN MATCHED THEN UPDATE SET KPI_ID = s.kid, CONDITION = s.cond, SEVERITY = s.sev, ENABLED = s.en WHEN NOT MATCHED THEN INSERT (ALERT_ID, KPI_ID, CONDITION, SEVERITY, ENABLED) VALUES (s.aid, s.kid, s.cond, s.sev, s.en);
$$

