-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.ADD_ACCOUNT_CREDIT_07(credit_id STRING, account_id STRING, amount NUMBER, currency STRING, expires_at TIMESTAMP_LTZ)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.ACCOUNT_CREDITS_07 (CREDIT_ID, ACCOUNT_ID, AMOUNT, CURRENCY, EXPIRES_AT) VALUES (:credit_id, :account_id, :amount, :currency, :expires_at);
$$

