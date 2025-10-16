# index_builder.py
# Builds FAISS index shards from embeddings snapshot files stored in S3, writes index snapshots back to S3 for container snapshot loader approach per your FAISS/ANN guidance @22 @23.
import faiss, numpy as np, os, boto3, json, uuid
s3 = boto3.client("s3")
def build_index_from_s3(s3_bucket, s3_prefix, dimension, index_type="HNSW32", shard_id=None):
    # download embeddings JSONL from s3_prefix, build faiss index for that shard
    # simplistic example; production code must stream, shard and persist id_map
    objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix).get('Contents', [])
    vectors = []
    id_map = []
    for obj in objects:
        key = obj['Key']
        body = s3.get_object(Bucket=s3_bucket, Key=key)['Body'].read().decode('utf-8')
        for line in body.splitlines():
            rec = json.loads(line)
            vectors.append(rec['embedding'])
            id_map.append(rec['id'])
    xb = np.array(vectors).astype('float32')
    if index_type.startswith("HNSW"):
        index = faiss.IndexHNSWFlat(dimension, 32)
    else:
        index = faiss.IndexFlatL2(dimension)
    index.add(xb)
    idx_file = f"faiss_index_shard_{shard_id or uuid.uuid4().hex}.index"
    faiss.write_index(index, idx_file)
    s3.upload_file(idx_file, s3_bucket, f"{s3_prefix}/indexes/{idx_file}")
    return {"index_key": f"{s3_prefix}/indexes/{idx_file}", "shard_id": shard_id}

