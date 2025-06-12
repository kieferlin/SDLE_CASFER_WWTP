import json
import os
import sys
from collections import defaultdict

# check if a year was provided
if len(sys.argv) < 2:
    print("Error: Please provide a year to process as a command-line argument.", file=sys.stderr)
    sys.exit(1)

# get the year from the command line
year = sys.argv[1]

# input and output paths
input_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/1_facility_csv_to_metadata_json"
output_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/2_split_by_year"

# define specific input directory for the given year
year_dir = os.path.join(input_root, year)

if not os.path.isdir(year_dir):
    print(f"Error: Input directory for year {year} not found at {year_dir}", file=sys.stderr)
    sys.exit(1)

print(f"Starting job for Year: {year}...")

# accumulate all data for the entire year first
year_data_by_pollutant = defaultdict(list)

# traverse each JSON file inside the year directory
for filename in sorted(os.listdir(year_dir)):
    if not filename.endswith(".json"):
        continue

    input_path = os.path.join(year_dir, filename)
    
    try:
        with open(input_path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"  ! Warning: Could not decode JSON from {filename}. Skipping.")
        continue

    # group entries by pollutant name into the year-wide dictionary
    for entry in data:
        pollutant = entry["pollutant"]
        year_data_by_pollutant[pollutant].append(entry)
    
    print(f"  Aggregated {filename}")

# write each pollutant file exactly ONCE for the whole year
output_dir = os.path.join(output_root, year)
os.makedirs(output_dir, exist_ok=True)

for pollutant, entries in year_data_by_pollutant.items():
    safe_name = pollutant.replace("/", "-").replace(" ", "_").replace(",", "")
    output_file = os.path.join(output_dir, f"{safe_name}.json")

    with open(output_file, "w") as f:
        json.dump(entries, f, indent=2)

print(f"\n Year {year} complete. Wrote {len(year_data_by_pollutant)} pollutant files to {output_dir}")