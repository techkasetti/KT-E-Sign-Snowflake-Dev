CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_POLICY_TEMPLATE(template_id STRING, target_account STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
-- implementation applies template rules to the account; simplified placeholder
INSERT INTO DOCGEN.POLICY_ENFORCEMENT_EVENTS (EVENT_ID, POLICY_ID, REQUEST_ID, OUTCOME) VALUES (UUID_STRING(), :template_id, :target_account, 'APPLIED');
$$;

