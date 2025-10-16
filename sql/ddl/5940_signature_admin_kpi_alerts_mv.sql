USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE MATERIALIZED VIEW DOCGEN.MV_KPI_ALERTS AS SELECT KPI_NAME, COUNT(*) AS TRIGGERS FROM DOCGEN.METRICS_ALERTS GROUP BY KPI_NAME; -- @1

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: -- Emitting next 130 Snowflake DDL artifacts for E-Sign module @1
