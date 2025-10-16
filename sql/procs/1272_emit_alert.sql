-- Emit an alert record for operational monitoring and paging. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.EMIT_ALERT(alert_type STRING, entity_ref STRING, details VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.SIGNATURE_ALERTS (ALERT_ID, ALERT_TYPE, ENTITY_REF, DETAILS) VALUES (UUID_STRING(), :alert_type, :entity_ref, :details); $$;

