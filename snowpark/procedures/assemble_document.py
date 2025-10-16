# assemble_document.py
# Snowpark stored-proc (demo) that assembles a document from template + clauses stored in tables
from snowflake.snowpark import Session
import uuid, json, datetime

def assemble_document(session: Session, account_id: str, template_id: str, merge_context: dict):
    """
    Assemble a document instance (demo): merge template text and clauses, persist Document_Instance and return assembly_run_id and a URL placeholder.
    """
    # Fetch template body (demo: simulate with a constructed string)
    template_body = f"Demo template {template_id} for account {account_id}.\n<<CLAUSES_PLACEHOLDER>>\n-- End of template."
    # Fetch clauses for template (demo static)
    clauses = session.sql(f"SELECT 'Clause A text.' AS clause_text UNION ALL SELECT 'Clause B text.'").collect()
    clause_texts = "\n\n".join([c['CLAUSE_TEXT'] for c in clauses])
    assembled_body = template_body.replace("<<CLAUSES_PLACEHOLDER>>", clause_texts)
    assembly_run_id = "assembly_" + str(uuid.uuid4())
    document_id = "doc_" + str(uuid.uuid4())
    # Persist minimal archive row
    session.sql(f"""
      INSERT INTO DOCGEN.DOCUMENT_ARCHIVE (DOCUMENT_ID, TEMPLATE_ID, ASSEMBLY_RUN_ID, DOCUMENT_URL, DOCUMENT_HASH, CREATED_BY, CREATED_AT)
      VALUES ('{document_id}', '{template_id}', '{assembly_run_id}', 's3://demo-bucket-12345/docs/{document_id}.pdf', '{uuid.uuid4().hex}', 'assembler-demo', CURRENT_TIMESTAMP());
    """).collect()
    # Optionally persist assembled content to EMBEDDINGS or parsed store (demo)
    session.sql(f"""
      INSERT INTO DOCGEN.DOCUMENT_EMBEDDINGS (DOCUMENT_ID, SECTION_ID, EMBEDDING, PROVENANCE, METADATA, CREATED_AT)
      VALUES ('{document_id}','body_1', PARSE_JSON('{{"vector":[0.1,0.2,0.3]}}'), PARSE_JSON('{{"source":"assembler"}}'), PARSE_JSON('{{"assembled_at":"{datetime.datetime.utcnow().isoformat()}"}}'), CURRENT_TIMESTAMP());
    """).collect()
    return {"assembly_run_id": assembly_run_id, "document_id": document_id, "document_url": f"s3://demo-bucket-12345/docs/{document_id}.pdf"}

