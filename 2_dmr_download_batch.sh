#!/bin/bash
#SBATCH --job-name=epa_download
#SBATCH --array=0-50 
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=216:00:00
#SBATCH --output=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download_logs/epa_%A_%a.out
#SBATCH --error=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download_logs/epa_%A_%a.err
#SBATCH --signal=USR1@300  # send signal 5 minutes before timeout
#SBATCH --requeue  # allow job to requeue on timeout

# define states
STATES=("AL" "AR" "AZ" "CA" "CO" "CT" "DC" "DE" "FL" "GA" "IA" "ID" "IL" "IN" "KS" 
"KY" "LA" "MA" "MD" "ME" "MI" "MN" "MO" "MS" "MT" "NC" "ND" "NE" "NH" "NJ" 
"NM" "NV" "NY" "OH" "OK" "OR" "PA" "RI" "SC" "SD" "TN" "TX" "UT" "VA" "VT" 
"WA" "WI" "WV" "WY")

# get the state based on SLURM array ID
STATE_INDEX=$SLURM_ARRAY_TASK_ID
STATE=${STATES[$STATE_INDEX]}

# --- CHANGE ---
# The master state check has been removed to allow for reruns within a year.
# The Python script will now handle all checkpointing logic internally.

echo "$(date): Starting processing for $STATE"

# Run the Python script for the current state
python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download.py "$STATE"

# Check if the script ran successfully
if [ $? -eq 0 ]; then
    echo "$(date): Python script completed successfully for $STATE"
else
    # The script failed. Exit with an error so Slurm marks it as FAILED
    # and does not attempt to requeue based on this failure.
    echo "$(date): ERROR processing $STATE. Check logs for details."
    exit 1
fi

# If job gets a timeout signal, allow requeue
# This trap will only be relevant if the job is about to hit its time limit.
trap 'echo "$(date): Job $SLURM_JOB_ID received SIGUSR1, exiting for requeue"; exit 0' SIGUSR1