import os
import json
from collections import defaultdict

input_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/pollutant_filtered_by_year"
output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/pollutant_filtered_merged"
os.makedirs(output_dir, exist_ok=True)

all_data = defaultdict(list)

for year in os.listdir(input_root):
    year_dir = os.path.join(input_root, year)
    if not os.path.isdir(year_dir):
        continue

    for filename in os.listdir(year_dir):
        if not filename.endswith(".json"):
            continue
        full_path = os.path.join(year_dir, filename)
        try:
            with open(full_path, "r") as f:
                data = json.load(f)
                all_data[filename].extend(data)
        except Exception as e:
            print(f"Error loading {full_path}: {e}")

for filename, records in all_data.items():
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)

print(f"Merged {len(all_data)} pollutants into {output_dir}")
