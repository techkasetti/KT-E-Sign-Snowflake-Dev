USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ACCOUNT_TEMPLATE_ENTITLEMENT_LOGS ( LOG_ID STRING PRIMARY KEY, ENT_ID STRING, ACTION STRING, ACTION_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), DETAILS VARIANT ); -- @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7671_signature_policy_versions.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_VERSIONS ( VERSION_ID STRING PRIMARY KEY, POLICY_ID STRING, VERSION_NUMBER INT, BODY VARIANT, APPLIED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @1

