-- Record a blockchain anchor for an evidence bundle after operator confirms on-chain transaction. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_BLOCKCHAIN_ANCHOR(bundle_id STRING, tx_id STRING, chain STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.BUNDLE_BLOCKCHAIN_ANCHORS (ANCHOR_ID, BUNDLE_ID, TX_ID, BLOCKCHAIN) VALUES (UUID_STRING(), :bundle_id, :tx_id, :chain);
$$;

