CREATE OR REPLACE PROCEDURE DOCGEN.REGISTER_KPI_DEFINITION_11(kpi_id STRING, name STRING, definition VARIANT, owner STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.KPI_DEFINITIONS_11 (KPI_ID, NAME, DEFINITION, OWNER) VALUES (:kpi_id, :name, :definition, :owner);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4690_signature_kpi_runs_11.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.KPI_RUNS_11 ( RUN_ID STRING PRIMARY KEY, KPI_ID STRING, RESULT VARIANT, RUN_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
