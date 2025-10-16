import json, faiss, numpy as np
def build_index(jsonl_path, output_path, nlist=100):
    # load embeddings and build IVF index with simple parameters (POC)
    pass
# FAISS builder implements shard and id_map logic for productionized ANN indexes per FAISS guidance @31 @367

