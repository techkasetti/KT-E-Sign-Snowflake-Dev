-- Helper SP to advance SIGNATURE_REQUESTS status when all signers have completed their steps per lifecycle orchestration patterns. @11 @31
CREATE OR REPLACE PROCEDURE DOCGEN.MARK_REQUEST_COMPLETED(request_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  UPDATE DOCGEN.SIGNATURE_REQUESTS
  SET STATUS = 'COMPLETED'
  WHERE REQUEST_ID = :request_id;
$$;

