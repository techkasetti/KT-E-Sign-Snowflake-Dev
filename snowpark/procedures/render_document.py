# render_document.py
# Snowpark stored-proc (demo) that simulates rendering HTML->PDF and marks rendered document
from snowflake.snowpark import Session
import uuid, datetime

def render_document(session: Session, document_id: str, renderer: str = "pdfium_demo"):
    """
    Simulate rendering by writing a render event and returning a signed URL placeholder.
    """
    rendered_url = f"s3://demo-bucket-12345/docs/{document_id}_rendered_{uuid.uuid4().hex}.pdf"
    session.sql(f"""
      INSERT INTO DOCGEN.DOCUMENT_ARCHIVE (DOCUMENT_ID, TEMPLATE_ID, ASSEMBLY_RUN_ID, DOCUMENT_URL, DOCUMENT_HASH, CREATED_BY, CREATED_AT)
      SELECT '{document_id}', TEMPLATE_ID, ASSEMBLY_RUN_ID, '{rendered_url}', '{uuid.uuid4().hex}', 'renderer-{renderer}', CURRENT_TIMESTAMP()
      FROM DOCGEN.DOCUMENT_ARCHIVE WHERE DOCUMENT_ID = '{document_id}' LIMIT 1;
    """).collect()
    # Mark signature request if exists
    session.sql(f"""
      UPDATE DOCGEN.SIGNATURE_REQUESTS SET STATUS = 'RENDERED' WHERE DOCUMENT_ID = '{document_id}' AND STATUS = 'PENDING';
    """).collect()
    return {"document_id": document_id, "rendered_url": rendered_url, "rendered_at": datetime.datetime.utcnow().isoformat()}

