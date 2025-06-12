import os

file_path = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download/2007/AL/ALG120162.csv"

if os.path.exists(file_path):
    print(f"File exists: {file_path}")
else:
    print(f"File does NOT exist: {file_path}")
