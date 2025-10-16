# Minimal assembly service: accepts POST /v1/assemble with JSON (assembly_run_id, document_bytes base64, signer_id, certs) then uploads document to S3 and calls Snowflake WRITE_EVIDENCE_BUNDLE via Snowflake connector. This mirrors the assembly + evidence uploader pattern in your design docs @186 @65.
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import base64, boto3, os, uuid, json
import snowflake.connector
app = FastAPI()
s3 = boto3.client("s3")
SNOW_ACCOUNT = os.environ.get("SNOW_ACCOUNT")
SNOW_USER = os.environ.get("SNOW_USER")
SNOW_ROLE = os.environ.get("SNOW_ROLE")
SNOW_WAREHOUSE = os.environ.get("SNOW_WAREHOUSE")
SNOW_DATABASE = os.environ.get("SNOW_DATABASE")
SNOW_SCHEMA = os.environ.get("SNOW_SCHEMA")
SNOW_PASSWORD = os.environ.get("SNOW_PASSWORD")
S3_BUCKET = os.environ.get("EVIDENCE_BUCKET")
class AssembleRequest(BaseModel):
    assembly_run_id: str
    document_base64: str
    document_name: str
    signer_id: str
    cert_chain: list
    metadata: dict = {}
@app.post("/v1/assemble")
def assemble(req: AssembleRequest):
    try:
        doc_bytes = base64.b64decode(req.document_base64)
        key = f"evidence/{req.assembly_run_id}/{uuid.uuid4().hex}_{req.document_name}"
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=doc_bytes)
        s3_url = f"s3://{S3_BUCKET}/{key}"
        # connect to Snowflake and call WRITE_EVIDENCE_BUNDLE
        ctx = snowflake.connector.connect(
            user=SNOW_USER,
            password=SNOW_PASSWORD,
            account=SNOW_ACCOUNT,
            role=SNOW_ROLE,
            warehouse=SNOW_WAREHOUSE,
            database=SNOW_DATABASE,
            schema=SNOW_SCHEMA
        )
        cs = ctx.cursor()
        try:
            cert_chain_json = json.dumps(req.cert_chain)
            metadata_json = json.dumps(req.metadata)
            cs.execute("CALL DOCGEN.WRITE_EVIDENCE_BUNDLE(%s,%s,%s,%s,PARSE_JSON(%s),%s)", (req.assembly_run_id, key, req.signer_id, 'sig_hash_placeholder', cert_chain_json, metadata_json))
            res = cs.fetchone()
        finally:
            cs.close()
            ctx.close()
        return {"status": "ok", "s3_url": s3_url, "snow_result": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

