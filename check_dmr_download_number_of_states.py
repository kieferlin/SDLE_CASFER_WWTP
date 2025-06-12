import os

# main directory where all the year folders are located.
BASE_DIR = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download"

#  complete set of states that SHOULD exist in each year's folder.
EXPECTED_STATES = {
    "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "IA", "ID", "IL", "IN", "KS", 
    "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", 
    "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", 
    "WA", "WI", "WV", "WY"
}

# main logic
def check_missing_states():
    """
    Scans the BASE_DIR for year folders and reports any missing state sub-folders.
    """
    print(f"Starting Check")
    print(f"Total Expected States: {len(EXPECTED_STATES)}\n")

    # ensure the base directory exists
    if not os.path.isdir(BASE_DIR):
        print(f"ERROR: The base directory was not found.")
        print(f"Please check the path: {BASE_DIR}")
        return

    # automatically find all directories inside BASE_DIR that look like years
    try:
        year_folders = sorted([
            entry.name for entry in os.scandir(BASE_DIR) 
            if entry.is_dir() and entry.name.isdigit() and len(entry.name) == 4
        ])
    except OSError as e:
        print(f"ERROR: Could not read the base directory. Check permissions. Details: {e}")
        return

    if not year_folders:
        print("RESULT: No year directories (e.g., '2007', '2008') were found in the base directory.")
        return

    # loop through each year we found and check its contents.
    for year in year_folders:
        year_path = os.path.join(BASE_DIR, year)
        
        # get the set of directories that actually exist in the year's folder.
        try:
            found_states = {
                entry.name for entry in os.scandir(year_path) if entry.is_dir()
            }
        except OSError:
            print(f"Year {year}: Could not scan directory. Skipping.")
            continue

        # use set difference to find what's missing
        missing_states = EXPECTED_STATES - found_states
        
        # report the results for this year
        if not missing_states:
            print(f"Year {year}: All {len(EXPECTED_STATES)} states are loaded")
        else:
            # sort the list of missing states for clean, alphabetical output
            sorted_missing = sorted(list(missing_states))
            print(f"Year {year}: MISSING {len(sorted_missing)} states -> {sorted_missing}")

if __name__ == "__main__":
    check_missing_states()