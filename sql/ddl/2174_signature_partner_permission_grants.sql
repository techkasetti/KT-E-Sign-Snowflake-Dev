USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PARTNER_PERMISSION_GRANTS ( GRANT_ID STRING PRIMARY KEY, PARTNER_ID STRING, PERMS VARIANT, GRANTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2175_signature_partner_onboarding_requests.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PARTNER_ONBOARDING_REQUESTS ( REQ_ID STRING PRIMARY KEY, PARTNER_NAME STRING, CONTACT VARIANT, STATUS STRING, REQUESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

