import sys
import os
from extract_unique_column_values import expand_from_all_files

# parse year and state from command-line arguments
year = sys.argv[1]
state = sys.argv[2]

# construct input and output paths
input_dir = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/EPA-WWTP/{year}/{state}"
output_path = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/output/{year}_{state}_unique.json"

# skip if already processed
if os.path.exists(output_path):
    print(f" Already processed: {year} / {state}, skipping.")
    sys.exit(0)

#  skip if input directory is missing
if not os.path.exists(input_dir):
    print(f" Input directory not found: {input_dir}")
    sys.exit(1)

# ensure output directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# process the directory
expand_from_all_files(input_dir, output_path=output_path)
