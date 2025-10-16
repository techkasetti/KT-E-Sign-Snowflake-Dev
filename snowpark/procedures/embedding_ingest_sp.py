# embedding_ingest_sp.py
from snowflake.snowpark import Session

def embedding_ingest(session: Session, staged_path: str):
    """
    Idempotent ingestion: COPY staged JSONL into TMP_EMBEDDINGS then MERGE into DOCUMENT_EMBEDDINGS.
    """
    session.sql(f"""
        COPY INTO DOCGEN.TMP_EMBEDDINGS
        FROM @AI_FEATURE_HUB.DOCGEN.EMBEDDINGS_STAGE/{staged_path}
        FILE_FORMAT=(FORMAT_NAME='AI_FEATURE_HUB.DOCGEN.JSONL_FORMAT')
    """).collect()

    session.sql("""
        MERGE INTO DOCGEN.DOCUMENT_EMBEDDINGS tgt
        USING DOCGEN.TMP_EMBEDDINGS src
        ON tgt.DOCUMENT_ID = src.DOCUMENT_ID AND tgt.SECTION_ID = src.SECTION_ID
        WHEN MATCHED THEN UPDATE SET EMBEDDING = src.EMBEDDING, PROVENANCE = src.PROVENANCE, METADATA = src.METADATA, CREATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (DOCUMENT_ID, SECTION_ID, EMBEDDING, PROVENANCE, METADATA, CREATED_AT)
          VALUES (src.DOCUMENT_ID, src.SECTION_ID, src.EMBEDDING, src.PROVENANCE, src.METADATA, CURRENT_TIMESTAMP());
    """).collect()

    session.sql("TRUNCATE TABLE IF EXISTS DOCGEN.TMP_EMBEDDINGS").collect()
    return {"status": "ok"}

