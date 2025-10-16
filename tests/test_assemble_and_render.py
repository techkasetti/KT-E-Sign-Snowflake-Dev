# test_assemble_and_render.py (pytest demo)
import os, json
from snowflake.connector import connect

def get_conn():
    return connect(
        user="sysadmin",
        account="demo_account",
        password="demo_password",
        role="SYSADMIN"
    )

def test_assemble_and_render_flow():
    conn = get_conn()
    cs = conn.cursor()
    # Assemble document
    cs.execute("CALL DOCGEN.ASSEMBLE_DOCUMENT('acct_demo','tpl_demo_1', PARSE_JSON('{\"name\":\"Alice\"}'))")
    # Simulate available document id by selecting recent doc
    cs.execute("SELECT DOCUMENT_ID FROM DOCGEN.DOCUMENT_ARCHIVE ORDER BY CREATED_AT DESC LIMIT 1")
    doc_id = cs.fetchone()[0]
    # Render document
    cs.execute(f"CALL DOCGEN.RENDER_DOCUMENT('{doc_id}', 'pdfium_demo')")
    # Check archive for rendered doc
    cs.execute(f"SELECT DOCUMENT_URL FROM DOCGEN.DOCUMENT_ARCHIVE WHERE DOCUMENT_ID = '{doc_id}' ORDER BY CREATED_AT DESC LIMIT 1")
    url = cs.fetchone()[0]
    assert url is not None

