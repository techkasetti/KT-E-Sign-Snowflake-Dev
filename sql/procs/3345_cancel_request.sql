CREATE OR REPLACE PROCEDURE DOCGEN.CANCEL_REQUEST(request_id STRING, cancelled_by STRING, reason STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.REQUEST_CANCELLATIONS (CAN_ID, REQUEST_ID, CANCELLED_BY, REASON) VALUES (UUID_STRING(), :request_id, :cancelled_by, :reason);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Emitting next 130 Snowflake artifacts (DDL + stored procedures) for the E-Signature module, following the Snowflake design and registration patterns in your workspace @31 @24 @6

