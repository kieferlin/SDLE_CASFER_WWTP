import os
import datetime

# base directory containing all the state folders
BASE_DIR = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download"

START_YEAR = 2000

# complete set of states
EXPECTED_STATES = {
    "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "IA", "ID", "IL", "IN", "KS", 
    "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", 
    "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", 
    "WA", "WI", "WV", "WY"
}

# main logic
def verify_state_year_csv_files():
    """
    Checks for the existence of a 'YYYY_SS.csv' file for every state
    and every year from START_YEAR to the current year.
    """
    
    # automatically determine the current year
    current_year = datetime.date.today().year
    
    print("--- Starting State-Year CSV File Verification ---")
    print(f"Checking for files from {START_YEAR} to {current_year}")
    print(f"Expected file format: 'YYYY_SS.csv' (e.g., '{START_YEAR}_AL.csv')\n")

    # list will store the full path of any missing files
    missing_files = []

    # ensure the main data directory exists
    if not os.path.isdir(BASE_DIR):
        print(f"FATAL ERROR: The base directory was not found: {BASE_DIR}")
        return

    # loop through every state (sorted alphabetically)
    for state_code in sorted(list(EXPECTED_STATES)):
        
        # loop through every year in the required range.
        for year in range(START_YEAR, current_year + 1):
            
            # construct the expected filename
            filename = f"{year}_{state_code}.csv"
            
            # cnstruct the full, absolute path to the file
            expected_path = os.path.join(BASE_DIR, state_code, filename)
            
            # check if the file actually exists
            if not os.path.isfile(expected_path):
                missing_files.append(expected_path)

    # final report
    if not missing_files:
        print("ALL expected state-year CSV files were found")
    else:
        print(f"FAILED, found {len(missing_files)} missing files:")
        for path in missing_files:
            print(f"  - {path}")
            
    print("\n Check complete")

if __name__ == "__main__":
    verify_state_year_csv_files()