-- Create document assembly job entry; used by front-end preview and sign flows. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.ENQUEUE_ASSEMBLY_JOB(template_id STRING, account_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.DOCUMENT_ASSEMBLY_JOBS(ASSEMBLY_ID, TEMPLATE_ID, ACCOUNT_ID, STATUS) VALUES (UUID_STRING(), :template_id, :account_id, 'PENDING'); $$;

