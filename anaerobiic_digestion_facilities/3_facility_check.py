import pandas as pd

# use the pre-filtered csv of WRRFs as starting point
wrrf_facilities_file = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/WRRF_anaerobic_digestion_facilities.csv"
matched_facilities_file = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/wrrf_fuzzy_matches.csv"

output_file = "unmatched_wrrf_facilities.csv"

try:
    print("Starting Unmatched Facility Finder")

    # load the pre-filtered list of WRRF facilities.
    print(f"Loading pre-filtered WRRFs from: {wrrf_facilities_file}")
    wrrf_df = pd.read_csv(wrrf_facilities_file, engine="python")
    print(f"-> Loaded {len(wrrf_df)} total WRRF facilities.")

    # load the list of facilities that have already been matched.
    print(f"Loading matched facilities from: {matched_facilities_file}")
    matched_df = pd.read_csv(matched_facilities_file, engine="python")
    print(f"-> Loaded {len(matched_df)} matched records.")

    # get the set of names that have already been matched.
    # 'ADF Name' column in your matches file holds the names to exclude
    matched_names = set(matched_df['ADF Name'])
    print(f"\nFound {len(matched_names)} unique matched facility names to exclude.")

    # filter the WRRF DataFrame to find ones NOT in the matched list
    print("Finding unmatched WRRFs...")

    # keeps rows where the 'Name' is NOT IN the `matched_names` set
    unmatched_wrrf_df = wrrf_df[~wrrf_df['Name'].isin(matched_names)]
    print(f"-> Found {len(unmatched_wrrf_df)} unmatched WRRFs.")

    # save the final list to a new CSV file
    unmatched_wrrf_df.to_csv(output_file, index=False)

    print("\n--- Success! ---")
    print(f"The list of {len(unmatched_wrrf_df)} unmatched WRRFs has been saved to: {output_file}")

except FileNotFoundError as e:
    print(f"\n--- ERROR ---")
    print(f"Could not find a file. Please check your file paths.")
    print(f"Details: {e}")
except KeyError as e:
    print(f"\n--- ERROR ---")
    print(f"A column name was not found in one of the CSVs: {e}")
    print("Please check that 'Name' (in the WRRF file) or 'ADF Name' (in the matches file) exist.")