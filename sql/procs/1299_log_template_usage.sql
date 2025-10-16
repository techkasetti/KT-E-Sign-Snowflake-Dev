-- Record a template usage event for downstream billing attribution. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.LOG_TEMPLATE_USAGE(template_id STRING, account_id STRING, context VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNATURE_TEMPLATE_USAGE_LOG (LOG_ID, TEMPLATE_ID, ACCOUNT_ID, CONTEXT) VALUES (UUID_STRING(), :template_id, :account_id, :context);
$$;

