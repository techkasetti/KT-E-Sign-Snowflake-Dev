# FAISS Operation Notes (generated)
- Place index snapshots under s3://faiss-index-bucket-prod/indices/docgen/faiss_index_v1/ with shard_N.index files. @63
- Use tools/build_and_push_faiss.sh to produce shards and register snapshot manifest into Snowflake via register_index_snapshot.py proc. @63 @296
- After registering snapshot, set up FAISS query service containers (Dockerfile provided) behind an API gateway and register the External Function DOCGEN.FAISS_SIMILARITY. @63 @4

