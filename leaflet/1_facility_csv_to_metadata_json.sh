#!/bin/bash
#SBATCH --job-name=multi_json
#SBATCH --output=1_csv_to_json_logs/multi_json_%A_%a.out
#SBATCH --error=logs/multi_json_%A_%a.err
#SBATCH --array=0-50
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=72:00:00

# list of states
STATES=("AL" "AR" "AZ" "CA" "CO" "CT" "DC" "DE" "FL" "GA" "IA" "ID" "IL" "IN" "KS" \
        "KY" "LA" "MA" "MD" "ME" "MI" "MN" "MO" "MS" "MT" "NC" "ND" "NE" "NH" "NJ" \
        "NM" "NV" "NY" "OH" "OK" "OR" "PA" "RI" "SC" "SD" "TN" "TX" "UT" "VA" "VT" \
        "WA" "WI" "WV" "WY")

STATE=${STATES[$SLURM_ARRAY_TASK_ID]}

echo "Running for state: $STATE"

python3 1_csv_to_json.py "$STATE"
