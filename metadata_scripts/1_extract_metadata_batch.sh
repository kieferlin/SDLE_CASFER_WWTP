#!/bin/bash
#SBATCH --job-name=meta_extract
#SBATCH --output=1_extract_metadata_logs/meta_%A_%a.out
#SBATCH --error=1_extract_metadata_logs/meta_%A_%a.err
#SBATCH --array=0-999%50         
#SBATCH --time=72:00:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

# safe bash behavior
set -euo pipefail

# ensure log directory exists
mkdir -p logs

# year and state setup
YEARS=($(seq 2007 2026))
STATES=("AL" "AR" "AZ" "CA" "CO" "CT" "DC" "DE" "FL" "GA" "IA" "ID" "IL" "IN" "KS" \
        "KY" "LA" "MA" "MD" "ME" "MI" "MN" "MO" "MS" "MT" "NC" "ND" "NE" "NH" "NJ" \
        "NM" "NV" "NY" "OH" "OK" "OR" "PA" "RI" "SC" "SD" "TN" "TX" "UT" "VA" "VT" \
        "WA" "WI" "WV" "WY")

# get the number of states for the calculation
NUM_STATES=${#STATES[@]}

# use the number of states for the calculation
YEAR_INDEX=$((SLURM_ARRAY_TASK_ID / NUM_STATES))
STATE_INDEX=$((SLURM_ARRAY_TASK_ID % NUM_STATES))

YEAR=${YEARS[$YEAR_INDEX]}
STATE=${STATES[$STATE_INDEX]}

echo "[$SLURM_ARRAY_TASK_ID] Processing $YEAR / $STATE"

python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/metadata_scripts/1_extract_metadata.py "$YEAR" "$STATE"