# snapshot_loader.py
# FAISS container entrypoint to download index snapshot (index file and id_map) from S3 and serve via REST or be called by External Function; follows snapshot-loader pattern in your specs @22 @23.
import faiss, os, boto3
s3 = boto3.client("s3")
def load_index_from_s3(s3_bucket, index_key):
    local_file = index_key.split("/")[-1]
    s3.download_file(s3_bucket, index_key, local_file)
    index = faiss.read_index(local_file)
    return index

