USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE MATERIALIZED VIEW DOCGEN.MV_INDEX_SNAPSHOTS AS SELECT SNAPSHOT_ID, LOCATION, CREATED_AT FROM DOCGEN.INDEX_SNAPSHOTS ORDER BY CREATED_AT DESC;

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/5871_signature_admin_alert_subscriptions.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_ALERT_SUBSCRIPTIONS ( SUB_ID STRING PRIMARY KEY, ALERT_TYPE STRING, SUBSCRIBER STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @1

