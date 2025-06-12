import os
import csv
import json
from collections import defaultdict

def expand_from_all_files(input_dir, output_path):
    """
    Scans all CSVs in input_dir and writes a fresh set of unique 
    column values to output_path (JSON), overwriting any previous file.
    """
    #  start with a fresh empty dictionary ensureing that old values from previous runs are never included
    unique_values = defaultdict(set)

    # scan all CSVs and build the unique value sets from scratch
    print(f" Regenerating unique values from all files in {input_dir}...")
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            filepath = os.path.join(input_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for _ in range(4):  # skip metadata lines
                        next(f)
                    reader = csv.DictReader(f)
                    for row in reader:
                        for col, val in row.items():
                            if col is not None and val is not None:
                                unique_values[col.strip()].add(val.strip())
            except Exception as e:
                print(f"Error reading {filepath}: {e}")

    # convert sets to sorted lists for consistent output and save
    unique_values_json = {col: sorted(list(vals)) for col, vals in unique_values.items()}
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(unique_values_json, f, indent=2)

    print(f" Wrote fresh unique column values to {output_path}")