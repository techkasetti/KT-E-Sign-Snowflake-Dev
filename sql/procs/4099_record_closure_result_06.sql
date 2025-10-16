CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_CLOSURE_RESULT_06(result_id STRING, task_id STRING, status STRING, output VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.CLOSURE_RESULTS_06 (RESULT_ID, TASK_ID, STATUS, OUTPUT) VALUES (:result_id, :task_id, :status, :output);
UPDATE DOCGEN.CLOSURE_TASKS_06 SET STATUS = :status WHERE TASK_ID = :task_id;
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: The artifacts below follow the Snowflake Snowpark, External Function, and evidence/PKI patterns in your workspace. @31 @24 @34

