#!/bin/bash
#SBATCH --job-name=facility_download
#SBATCH --output=facility_download_%j.out
#SBATCH --error=facility_download_%j.err
#SBATCH --time=48:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G

# Run the script
python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/download_facility.py
