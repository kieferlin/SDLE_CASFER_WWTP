import os
import json

# Root directory containing year-wise metadata folders
root_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/final_metadata_files"

# Set to collect all unique Parameter Descriptions
unique_parameters = set()

# Walk through the directory tree
for dirpath, _, filenames in os.walk(root_dir):
    for filename in filenames:
        if filename.endswith("_description.json"):
            file_path = os.path.join(dirpath, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    param_desc = data.get("Parameter Description", [])
                    if isinstance(param_desc, list):
                        unique_parameters.update(param_desc)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

# Convert set to sorted list
unique_param_list = sorted(unique_parameters)

# Define output path
output_json = os.path.join(root_dir, "comprehensive_list_wwtp_discharge.json")

# Write to JSON file
with open(output_json, 'w', encoding='utf-8') as out:
    json.dump(unique_param_list, out, indent=2)

print(f"Saved {len(unique_param_list)} unique parameter descriptions to: {output_json}")
