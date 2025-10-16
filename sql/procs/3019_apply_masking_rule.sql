CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_MASKING_RULE(rule_id STRING, target_ref STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.MASKING_APPLICATIONS (APP_ID, RULE_ID, TARGET_REF) VALUES (UUID_STRING(), :rule_id, :target_ref);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts output at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Generating next tranche of Snowflake DDL + stored-procedures for the E-Signature module following the Snowpark / External Function / evidence & PKI patterns in your design materials @31 @24

