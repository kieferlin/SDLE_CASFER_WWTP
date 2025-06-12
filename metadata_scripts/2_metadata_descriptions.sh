#!/bin/bash
#SBATCH --job-name=meta_json
#SBATCH --output=2_metadata_descriptions_logs/meta_json_%j.out
#SBATCH --error=2_metadata_descriptions_logs/meta_json_%j.err
#SBATCH --time=00:10:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1


echo "Starting metadata generation..."
python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/2_metadata_descriptions.py
echo "Done."
