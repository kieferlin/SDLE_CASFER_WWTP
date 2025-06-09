import os
import json

# directory containing all json files
input_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/output"
base_output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/final_metadata_files"

# field descriptions (as comments)
column_descriptions = {
    "NPDES Permit Number": "A nine-character code used to uniquely identify a permitted NPDES facility (NPDES ID)...",
    "Outfall Number": "A three-character code in ICIS-NPDES that identifies the point of discharge (e.g., outfall)...",
    "Monitoring Location Code": "A single-character code in ICIS-NPDES that indicates the sampling location...",
    "Limit Set Designator": "A single-character code in ICIS-NPDES that groups limits that are set on a similar basis...",
    "Parameter Code": "A five-character code in ICIS-NDPES that identifies the regulated pollutant parameter...",
    "Parameter Description": "Description/parameter name that corresponds to the five-digit parameter code...",
    "Monitoring Period Date": "Time period (month and year) for the permit limit and reported discharge data...",
    "Limit Value": "The numeric limit from the Permit or Enforcement Action Final Order...",
    "Limit Value Unit": "The units for numeric limits from the Permit or Enforcement Action Final Order...",
    "DMR Value Type": "Indicates whether the DMR data are reported as concentrations (C1, C2, or C3) or quantities...",
    "Statistical Base": "The code representing the unit of measure applicable to the limit and DMR values...",
    "Limit Type Code": "The unique code that indicates whether a limit is an enforceable limit (ENF)...",
    "DMR Value": "The reported measurement value or No Data Indicator (NODI) code...",
    "DMR Value Unit": "The units for reported measurement values...",
    "dmr_comments": "Comments reported with the submission of the DMR."
}

# loop through each json in the input folder
for filename in os.listdir(input_dir):
    if filename.endswith("_unique.json"):
        input_path = os.path.join(input_dir, filename)

        # load data
        with open(input_path, "r") as f:
            all_fields = json.load(f)

        # extract Parameter Description only
        param_values = all_fields.get("Parameter Description", [])

        # build output JSON with only Parameter Description and comments
        output = {}
        for col, desc in column_descriptions.items():
            if col == "Parameter Description":
                output[col] = param_values
            else:
                output[f"_comment_{col}"] = desc

        # extract year from filename (e.g., "2007_AL_unique.json" → "2007")
        base = filename.replace("_unique.json", "")
        year = base.split("_")[0]

        # build output directory for that year
        year_output_dir = os.path.join(base_output_dir, year)
        os.makedirs(year_output_dir, exist_ok=True)

        # write to that year-specific directory
        output_filename = f"metadata_{base}_description.json"
        output_path = os.path.join(year_output_dir, output_filename)

        with open(output_path, "w") as out_file:
            json.dump(output, out_file, indent=2)

        print(f"Created {output_filename} in {year_output_dir}")
