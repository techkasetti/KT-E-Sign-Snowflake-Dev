CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ADMIN_PERMISSION_CHANGE(admin_user STRING, action STRING, target STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.ADMIN_PERMISSIONS_LOG (LOG_ID, ADMIN_USER, ACTION, TARGET) VALUES (UUID_STRING(), :admin_user, :action, :target);
$$;

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Generated following the Snowflake patterns and delivery runbook in your workspace @31 @24 @52

