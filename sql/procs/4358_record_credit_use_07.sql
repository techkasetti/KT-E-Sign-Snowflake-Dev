-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_CREDIT_USE_07(use_id STRING, credit_id STRING, bundle_id STRING, amount_used NUMBER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.CREDIT_USES_07 (USE_ID, CREDIT_ID, BUNDLE_ID, AMOUNT_USED) VALUES (:use_id, :credit_id, :bundle_id, :amount_used);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4288_signature_webhook_subscriptions_07.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.WEBHOOK_SUBSCRIPTIONS_07 ( SUB_ID STRING PRIMARY KEY, ACCOUNT_ID STRING, TARGET_URL STRING, EVENTS ARRAY, SECRET_HASH STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

