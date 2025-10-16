-- Record policy change with actor for compliance traceability. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_POLICY_CHANGE(policy_id STRING, changed_by STRING, change_payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNING_POLICY_AUDIT (AUDIT_ID, POLICY_ID, CHANGED_BY, CHANGE_PAYLOAD) VALUES (UUID_STRING(), :policy_id, :changed_by, :change_payload);
$$;

