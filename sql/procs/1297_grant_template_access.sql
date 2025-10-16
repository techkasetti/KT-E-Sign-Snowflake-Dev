-- Grant access to a principal for a template and record audit. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.GRANT_TEMPLATE_ACCESS(template_id STRING, principal STRING, role STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNATURE_TEMPLATE_ACL (ACL_ID, TEMPLATE_ID, PRINCIPAL, ROLE) VALUES (UUID_STRING(), :template_id, :principal, :role);
$$;

