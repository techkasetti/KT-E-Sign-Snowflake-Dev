USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ENTITLEMENT_ROLLBACK_AUDIT ( AUDIT_ID STRING PRIMARY KEY, TENANT_ID STRING, FEATURE_NAME STRING, PREV_STATE VARIANT, ROLLED_BACK_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), ROLLBACK_BY STRING );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: -- Generated per Snowflake E-Signature patterns @31
