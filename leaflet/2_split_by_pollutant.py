import json
import os
from collections import defaultdict

# New input directory structure: .../multi_year/{YEAR}/{STATE}_{YEAR}.json
input_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/multi_year"
output_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/pollutant_filtered_by_year"

# Ensure output root directory exists
os.makedirs(output_root, exist_ok=True)

# Traverse each year directory
for year in sorted(os.listdir(input_root)):
    year_dir = os.path.join(input_root, year)
    if not os.path.isdir(year_dir):
        continue  # Skip non-directory files

    output_dir = os.path.join(output_root, year)
    os.makedirs(output_dir, exist_ok=True)

    # Traverse each JSON file inside the year directory
    for filename in sorted(os.listdir(year_dir)):
        if not filename.endswith(".json"):
            continue

        input_path = os.path.join(year_dir, filename)

        # Load state-year JSON data
        with open(input_path, "r") as f:
            data = json.load(f)

        # Group entries by pollutant name
        by_pollutant = defaultdict(list)
        for entry in data:
            pollutant = entry["pollutant"]
            by_pollutant[pollutant].append(entry)

        # Save each pollutant group to its own JSON file
        for pollutant, entries in by_pollutant.items():
            safe_name = pollutant.replace("/", "-").replace(" ", "_").replace(",", "")
            output_file = os.path.join(output_dir, f"{safe_name}.json")

            # If file exists (i.e., from another state in same year), append
            if os.path.exists(output_file):
                with open(output_file, "r") as f:
                    existing = json.load(f)
                entries = existing + entries

            # Write merged or new entries
            with open(output_file, "w") as f:
                json.dump(entries, f, indent=2)

        print(f"✓ Year {year}, File {filename} → {len(by_pollutant)} pollutant files → {output_dir}")
