-- Append operator action to escalation log for incident tracking. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_OPERATOR_ACTION(alert_id STRING, operator STRING, action_taken STRING, notes VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.OPERATOR_ESCALATION_LOG (LOG_ID, ALERT_ID, OPERATOR, ACTION_TAKEN, NOTES) VALUES (UUID_STRING(), :alert_id, :operator, :action_taken, :notes);
$$;

