-- Create cross-tenant link record following admin approval flows. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_CROSS_TENANT_LINK(src_account STRING, tgt_account STRING, policy_ref STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNATURE_CROSS_TENANT_LINKS (LINK_ID, SOURCE_ACCOUNT_ID, TARGET_ACCOUNT_ID, POLICY_REF) VALUES (UUID_STRING(), :src_account, :tgt_account, :policy_ref);
$$;

