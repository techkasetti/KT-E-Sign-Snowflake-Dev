-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_SESSION_TOKEN_07(token_id STRING, session_id STRING, token_hash STRING, expires_at TIMESTAMP_LTZ)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SESSION_TOKENS_07 (TOKEN_ID, SESSION_ID, TOKEN_HASH, EXPIRES_AT) VALUES (:token_id, :session_id, :token_hash, :expires_at);
$$

