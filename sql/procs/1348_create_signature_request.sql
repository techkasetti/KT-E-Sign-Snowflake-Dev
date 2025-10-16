CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_SIGNATURE_REQUEST(document_id STRING, account_id STRING, created_by STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
def handler(session, document_id, account_id, created_by):
    import uuid
    req_id = 'req-' + str(uuid.uuid4())[:8]
    session.sql(f"INSERT INTO DOCGEN.SIGNATURE_REQUESTS(REQUEST_ID,DOCUMENT_ID,ACCOUNT_ID,CREATED_BY,STATUS) VALUES('{req_id}','{document_id}','{account_id}','{created_by}','PENDING')").collect()
    return {'request_id': req_id}
$$;

