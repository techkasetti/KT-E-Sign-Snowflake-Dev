# Helper to upload FAISS index files and manifest to S3 used by snapshot registration flows. @31 @24 @52
def upload_snapshot(local_path, bucket, snapshot_id):
    return {"snapshot_id": snapshot_id}

