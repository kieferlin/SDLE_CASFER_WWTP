import os
import csv

input_base = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/facility_data"
output_base = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/EPA-WWTP"

missing_report = {}

for state in os.listdir(input_base):
    state_folder = os.path.join(input_base, state)
    if not os.path.isdir(state_folder):
        continue

    for filename in os.listdir(state_folder):
        if not filename.endswith(f"_{state}.csv"):
            continue

        year = filename.split("_")[0]
        input_path = os.path.join(state_folder, filename)
        output_folder = os.path.join(output_base, year, state)

        # Load expected NPDES IDs
        npdes_ids = set()
        with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
            for _ in range(3):
                next(f)
            reader = csv.DictReader(line.replace('\x00', '') for line in f)
            for row in reader:
                npdes_id = row.get('NPDES Permit Number', '').strip()
                if npdes_id:
                    npdes_ids.add(npdes_id)

        # Check for missing files
        missing_ids = []
        for npdes_id in npdes_ids:
            expected_file = os.path.join(output_folder, f"{npdes_id}.csv")
            if not os.path.exists(expected_file):
                missing_ids.append(npdes_id)

        if missing_ids:
            missing_report[f"{state}_{year}"] = missing_ids

# Report
for key, ids in missing_report.items():
    print(f"{key}: {len(ids)} missing")
    for npdes_id in ids:
        print(f"  - {npdes_id}")
