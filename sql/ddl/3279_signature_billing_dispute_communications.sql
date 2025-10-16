USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.BILLING_DISPUTE_COMMUNICATIONS ( COMM_ID STRING PRIMARY KEY, DISPUTE_ID STRING, SENDER STRING, MESSAGE CLOB, SENT_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3280_signature_payment_disputes.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PAYMENT_DISPUTES ( DISPUTE_ID STRING PRIMARY KEY, INVOICE_ID STRING, ACCOUNT_ID STRING, AMOUNT NUMBER, REASON CLOB, STATUS STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

