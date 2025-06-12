import os
import shutil

EPA_DIR = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download"
TRASH_DIR = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/clean_bad_csvs_trash"
CHECKPOINT_DIR = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/checkpoints"
LOG_FILE = "clean_bad_csvs_logs.txt"

# ensure trash and log directories exist
os.makedirs(TRASH_DIR, exist_ok=True)
deleted = []

# identify and move bad CSVs
for root, _, files in os.walk(EPA_DIR):
    for file in files:
        if file.endswith(".csv"):
            full_path = os.path.join(root, file)
            try:
                with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read(1500).lower()
                    if "<html" in content or "file output failed" in content:
                        rel_path = os.path.relpath(full_path, EPA_DIR)
                        trash_path = os.path.join(TRASH_DIR, rel_path)
                        os.makedirs(os.path.dirname(trash_path), exist_ok=True)
                        shutil.move(full_path, trash_path)
                        deleted.append((rel_path, full_path))
                        print(f"Moved bad CSV to trash: {rel_path}")
            except Exception as e:
                print(f"Error reading {full_path}: {e}")

# edit checkpoint files to remove deleted NPDES IDs
for rel_path, full_path in deleted:
    try:
        parts = rel_path.split(os.sep)
        if len(parts) >= 3:
            year = parts[0]
            state = parts[1]
            npdes_id = os.path.splitext(parts[2])[0]

            checkpoint_file = os.path.join(CHECKPOINT_DIR, f"{state}_{year}_progress.txt")
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, "r") as f:
                    lines = [line.strip() for line in f if line.strip() != npdes_id]

                with open(checkpoint_file, "w") as f:
                    f.write("\n".join(lines) + "\n")
                print(f"Removed {npdes_id} from checkpoint: {checkpoint_file}")
    except Exception as e:
        print(f"Error editing checkpoint for {rel_path}: {e}")

# log deletions
if deleted:
    with open(LOG_FILE, "w") as f:
        for rel_path, _ in deleted:
            f.write(rel_path + "\n")
    print(f"\nDone. Moved {len(deleted)} bad files and updated checkpoints.")
else:
    print("No bad files found.")
