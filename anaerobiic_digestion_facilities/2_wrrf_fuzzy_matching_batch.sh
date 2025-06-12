#!/bin/bash
#SBATCH --job-name=fuzzy-matching     
#SBATCH --output=2_wrrf_fuzzy_matching_output%j.log
#SBATCH --error=2_wrrf_fuzzy_matching_error_%j.log         
#SBATCH --cpus-per-task=2                 
#SBATCH --mem=8gb                         
#SBATCH --time=72:00:00


# create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# install required packages
pip install --upgrade pip
pip install pandas rapidfuzz

# run script
python /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/2_wrrf_fuzzy_matching.py

# deactivate and remove  virtual environment
deactivate
rm -rf venv