-- Generated per Snowflake E-Signature patterns @31 @36
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ANCHOR_TX_07(tx_id STRING, bundle_id STRING, chain STRING, tx_hash STRING, published_at TIMESTAMP_LTZ)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.ANCHOR_TRANSACTIONS_07 (TX_ID, BUNDLE_ID, CHAIN, TX_HASH, PUBLISHED_AT) VALUES (:tx_id, :bundle_id, :chain, :tx_hash, :published_at);
$$

