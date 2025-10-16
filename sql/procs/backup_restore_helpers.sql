Purpose: SQL procedures to snapshot and restore key tables (backup/clone helpers) used in disaster recovery playbook. @241
-- backup_restore_helpers.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.SNAPSHOT_TABLE(table_name STRING, snapshot_prefix STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
EXECUTE IMMEDIATE
 'CREATE TABLE IF NOT EXISTS DOCGEN.' || snapshot_prefix || '_' || table_name || ' AS SELECT * FROM DOCGEN.' || table_name || ';';
RETURN 'snapshot_created';
$$;
CREATE OR REPLACE PROCEDURE DOCGEN.RESTORE_TABLE(snapshot_name STRING, target_table STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
EXECUTE IMMEDIATE
 'INSERT INTO DOCGEN.' || target_table || ' SELECT * FROM DOCGEN.' || snapshot_name || ';';
RETURN 'restore_completed';
$$;

