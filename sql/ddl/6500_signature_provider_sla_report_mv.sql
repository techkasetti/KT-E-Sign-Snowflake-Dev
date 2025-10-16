USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE MATERIALIZED VIEW DOCGEN.MV_PROVIDER_SLA_REPORT AS SELECT PROVIDER, AVG((DETAILS:latency)::NUMBER) AS AVG_LATENCY, SUM(CASE WHEN STATUS='FAIL' THEN 1 ELSE 0 END) AS FAILS FROM DOCGEN.PROVIDER_API_LOGS GROUP BY PROVIDER; -- @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6501_signature_provider_sla_history.sql @31
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PROVIDER_SLA_HISTORY ( HISTORY_ID STRING PRIMARY KEY, PROVIDER STRING, SLA_JSON VARIANT, APPLIED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

