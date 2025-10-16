# register_index_snapshot.py
# Registers an index snapshot manifest row in Snowflake (idempotent insert/update) @63 @296
from snowflake.snowpark import Session
import uuid, json, datetime

def register_index_snapshot(session: Session, snapshot_id: str, index_name: str, s3_prefix: str, shard_count: int, index_version: str, checksum: str):
    """
    Record or upsert a FAISS index snapshot manifest.
    Returns manifest_id and status.
    """
    # Idempotent upsert into INDEX_SNAPSHOT_MANIFEST
    session.sql(f"""
        MERGE INTO DOCGEN.INDEX_SNAPSHOT_MANIFEST tgt
        USING (SELECT '{snapshot_id}' AS SNAPSHOT_ID, '{index_name}' AS INDEX_NAME, '{s3_prefix}' AS S3_PREFIX, {shard_count} AS SHARD_COUNT, '{index_version}' AS INDEX_VERSION, '{checksum}' AS CHECKSUM) src
        ON tgt.SNAPSHOT_ID = src.SNAPSHOT_ID
        WHEN MATCHED THEN UPDATE SET S3_PREFIX = src.S3_PREFIX, SHARD_COUNT = src.SHARD_COUNT, INDEX_VERSION = src.INDEX_VERSION, CHECKSUM = src.CHECKSUM, CREATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (SNAPSHOT_ID, INDEX_NAME, S3_PREFIX, SHARD_COUNT, INDEX_VERSION, CHECKSUM) VALUES (src.SNAPSHOT_ID, src.INDEX_NAME, src.S3_PREFIX, src.SHARD_COUNT, src.INDEX_VERSION, src.CHECKSUM);
    """).collect()

    return {"snapshot_id": snapshot_id, "status": "registered", "registered_at": datetime.datetime.utcnow().isoformat()}

