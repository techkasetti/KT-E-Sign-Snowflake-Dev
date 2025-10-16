-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ADMIN_ACTION_07(action_id STRING, admin_id STRING, action STRING, target_ref STRING, notes STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.ADMIN_ACTIONS_07 (ACTION_ID, ADMIN_ID, ACTION, TARGET_REF, NOTES) VALUES (:action_id, :admin_id, :action, :target_ref, :notes);
$$

