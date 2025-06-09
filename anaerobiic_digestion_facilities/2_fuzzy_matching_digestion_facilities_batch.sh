#!/bin/bash
#SBATCH --job-name=facility-matching     
#SBATCH --output=output_%j.log          
#SBATCH --error=error_%j.log             
#SBATCH --cpus-per-task=2                 
#SBATCH --mem=8gb                         
#SBATCH --time=72:00:00                  

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip to the latest version
pip install --upgrade pip

# Install required packages
pip install pandas rapidfuzz

# Run your Python script
python /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/2_fuzzy_matching_digestion_facilities.py

# Deactivate and remove the virtual environment
deactivate
rm -rf venv