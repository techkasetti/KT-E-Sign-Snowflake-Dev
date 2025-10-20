import os

# Path to your target directory
folder_path = r"ddl"

for filename in os.listdir(folder_path):
    if filename.endswith(".sql") or ".sql " in filename:
        old_path = os.path.join(folder_path, filename)

        # Extract the portion before ".sql"
        new_name = filename.split(".sql")[0] + ".sql"
        new_path = os.path.join(folder_path, new_name)

        # Rename only if the name actually changes
        if old_path != new_path:
            os.rename(old_path, new_path)
            print(f"Renamed: {filename} → {new_name}")

print("✅ File name cleanup completed.")
