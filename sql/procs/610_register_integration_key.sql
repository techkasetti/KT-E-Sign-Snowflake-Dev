-- Example Snowflake proc to store only the salted hash for integration keys (show-once pattern) per security guidance. @124 @16
CREATE OR REPLACE PROCEDURE DOCGEN.REGISTER_INTEGRATION_KEY(account_id STRING, integration_key_hash STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  INSERT INTO DOCGEN.ACCOUNTS_INTEGRATION_KEYS(ACCOUNT_ID, INTEGRATION_KEY_HASH, CREATED_AT)
  VALUES (:account_id, :integration_key_hash, CURRENT_TIMESTAMP());
$$;

