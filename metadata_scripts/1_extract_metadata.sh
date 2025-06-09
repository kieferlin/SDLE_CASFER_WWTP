#!/bin/bash
#SBATCH --job-name=meta_extract
#SBATCH --output=logs/meta_%A_%a.out
#SBATCH --error=logs/meta_%A_%a.err
#SBATCH --array=0-930%20          # Throttle: max 20 jobs run at a time
#SBATCH --time=24:00:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

# safe bash behavior
set -euo pipefail

# ensure log directory exists
mkdir -p logs

# YEAR and STATE setup
YEARS=($(seq 2007 2026))
STATES=("AL" "AR" "AZ" "CA" "CO" "CT" "DC" "DE" "FL" "GA" "IA" "ID" "IL" "IN" "KS" 
        "KY" "LA" "MA" "MD" "ME" "MI" "MN" "MO" "MS" "MT" "NC" "ND" "NE" "NH" "NJ" 
        "NM" "NV" "NY" "OH" "OK" "OR" "PA" "RI" "SC" "SD" "TN" "TX" "UT" "VA" "VT" 
        "WA" "WI" "WV" "WY")

YEAR_INDEX=$((SLURM_ARRAY_TASK_ID / 49))
STATE_INDEX=$((SLURM_ARRAY_TASK_ID % 49))

YEAR=${YEARS[$YEAR_INDEX]}
STATE=${STATES[$STATE_INDEX]}

echo "[$SLURM_ARRAY_TASK_ID] Processing $YEAR / $STATE"

python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/1_extract_metadata.py "$YEAR" "$STATE"
