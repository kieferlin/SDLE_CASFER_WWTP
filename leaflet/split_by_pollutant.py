import json
import os
from collections import defaultdict

# input directory containing all {STATE}_{YEAR}.json from csv_to_json.py
input_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/multi_year"

output_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/pollutant_filtered_by_year"

# ensure output directory exists
os.makedirs(output_root, exist_ok=True)

# loop through all JSON files in the input directory
for filename in sorted(os.listdir(input_dir)):
    if not filename.endswith(".json"):
        continue  # Skip non-JSON files

    # Expect filenames like "AL_2007.json" → extract state and year
    parts = filename.replace(".json", "").split("_")
    if len(parts) != 2:
        continue  # Skip if not in expected format

    state, year = parts
    input_path = os.path.join(input_dir, filename)
    output_dir = os.path.join(output_root, year)
    os.makedirs(output_dir, exist_ok=True)  # Make sure year folder exists

    # Load state-year JSON data
    with open(input_path, "r") as f:
        data = json.load(f)

    # Group all entries by pollutant name
    by_pollutant = defaultdict(list)
    for entry in data:
        pollutant = entry["pollutant"]
        by_pollutant[pollutant].append(entry)

    # Save each pollutant group to its own JSON file in the appropriate year directory
    for pollutant, entries in by_pollutant.items():
        # Sanitize pollutant name for safe file naming
        safe_name = pollutant.replace("/", "-").replace(" ", "_").replace(",", "")
        output_file = os.path.join(output_dir, f"{safe_name}.json")

        # If the pollutant file already exists (from another state), append to it
        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                existing = json.load(f)
            entries = existing + entries  # Merge entries

        # Write the merged/new entries to file
        with open(output_file, "w") as f:
            json.dump(entries, f, indent=2)

    # Log success for this file
    print(f"✓ Year {year}, State {state} → {len(by_pollutant)} pollutant files → {output_dir}")
