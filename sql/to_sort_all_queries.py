import os
import re

# Directory containing all .sql files
SQL_DIR = "ddl"

# Output file for unique queries
OUTPUT_FILE = "sorted_queries.sql"

# Regex pattern to capture most SQL statements ending with semicolon
SQL_STATEMENT_PATTERN = re.compile(
    r"(?is)(?:CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|MERGE|TRUNCATE|GRANT|REVOKE|COMMENT|CALL|USE|COPY|DESCRIBE|SHOW)[\s\S]*?;",
    re.MULTILINE
)

def extract_sql_statements_from_file(file_path):
    """Extract SQL statements from a single file."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    # Normalize whitespace
    content = re.sub(r'\s+', ' ', content.strip())
    statements = SQL_STATEMENT_PATTERN.findall(content)
    return [stmt.strip() for stmt in statements]

def extract_unique_sql_statements(directory):
    """Extract and deduplicate SQL statements from all .sql files in a directory."""
    unique_statements = set()

    for root, _, files in os.walk(directory):
        for file_name in files:
            if file_name.lower().endswith('.sql'):
                file_path = os.path.join(root, file_name)
                statements = extract_sql_statements_from_file(file_path)
                for stmt in statements:
                    unique_statements.add(stmt)

    return unique_statements

def save_unique_statements(statements, output_file):
    """Save unique SQL statements to a file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        for stmt in sorted(statements):
            f.write(stmt + "\n\n")

if __name__ == "__main__":
    unique_queries = extract_unique_sql_statements(SQL_DIR)
    print(f"✅ Total unique queries extracted: {len(unique_queries)}")

    save_unique_statements(unique_queries, OUTPUT_FILE)
    print(f"✅ Unique queries saved to: {OUTPUT_FILE}")
