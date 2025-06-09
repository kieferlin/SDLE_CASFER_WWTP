import sys
import os
from extract_unique_column_values import expand_from_all_files

year = sys.argv[1]
state = sys.argv[2]

input_dir = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/EPA-WWTP/{year}/{state}"
output_path = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/output/{year}_{state}_unique.json"

# Check if input exists
if not os.path.exists(input_dir):
    print(f"Input directory not found: {input_dir}")
    sys.exit(1)

# If output exists, compare modification times
if os.path.exists(output_path):
    input_mtime = max(os.path.getmtime(os.path.join(input_dir, f)) for f in os.listdir(input_dir))
    output_mtime = os.path.getmtime(output_path)
    if output_mtime >= input_mtime:
        print(f"Output up-to-date for {year} / {state}, skipping.")
        sys.exit(0)

os.makedirs(os.path.dirname(output_path), exist_ok=True)
expand_from_all_files(input_dir, output_path=output_path)
