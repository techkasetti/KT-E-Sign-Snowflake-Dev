import os
import hashlib

# Path to your DDL folder
BASE_DIR = r"ddl"
OUTPUT_FILE = os.path.join(BASE_DIR, "unique_queries.sql")

# Use a set to store unique SQL statements by hash (for memory efficiency)
unique_hashes = set()
unique_queries = []

def normalize_sql(query: str) -> str:
    """
    Normalize SQL by trimming whitespace, removing redundant spaces, and lowercasing.
    This helps identify duplicates that differ only by spacing or case.
    """
    return " ".join(query.strip().split()).lower()

# Walk through all SQL files in the folder (non-recursive, can easily make recursive if needed)
for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".sql"):
            file_path = os.path.join(root, file)
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

                    # Split by semicolon (;) to separate multiple queries in a single file
                    queries = [q.strip() for q in content.split(";") if q.strip()]
                    for query in queries:
                        normalized = normalize_sql(query)
                        hash_key = hashlib.md5(normalized.encode()).hexdigest()

                        # Store only unique queries
                        if hash_key not in unique_hashes:
                            unique_hashes.add(hash_key)
                            unique_queries.append(query.strip() + ";")
            except Exception as e:
                print(f"⚠️ Error reading {file_path}: {e}")

# Write all unique queries to an output file
with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
    out.write("\n\n".join(unique_queries))

print(f"✅ Extraction complete: {len(unique_queries)} unique queries saved to {OUTPUT_FILE}")
