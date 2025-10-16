CREATE OR REPLACE PROCEDURE DOCGEN.TERMINATE_SESSION_06(term_id STRING, session_id STRING, reason STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SESSION_TERMINATIONS_06 (TERM_ID, SESSION_ID, REASON) VALUES (:term_id, :session_id, :reason);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Emitting next 130 Snowflake artifacts (DDL + stored procedures) following the Snowflake E-Signature patterns in your workspace. @1 @31

