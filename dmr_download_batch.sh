#!/bin/bash
#SBATCH --job-name=epa_download
#SBATCH --array=0-49%10  # One job per state, 10 at a time
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=72:00:00
#SBATCH --output=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/logs/epa_%A_%a.out
#SBATCH --error=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/logs/epa_%A_%a.err
#SBATCH --signal=USR1@300  # Send signal 5 minutes before timeout
#SBATCH --requeue  # Allow job to requeue on timeout

# Define states
STATES=("AL" "AR" "AZ" "CA" "CO" "CT" "DC" "DE" "FL" "GA" "IA" "ID" "IL" "IN" "KS" 
"KY" "LA" "MA" "MD" "ME" "MI" "MN" "MO" "MS" "MT" "NC" "ND" "NE" "NH" "NJ" 
"NM" "NV" "NY" "OH" "OK" "OR" "PA" "RI" "SC" "SD" "TN" "TX" "UT" "VA" "VT" 
"WA" "WI" "WV" "WY")

# Get the state based on SLURM array ID
STATE_INDEX=$SLURM_ARRAY_TASK_ID
STATE=${STATES[$STATE_INDEX]}

# Checkpoint file path
CHECKPOINT_FILE="/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/checkpoints/${STATE}_progress.txt"

# If checkpoint exists and state is fully processed, skip it
if [ -s "$CHECKPOINT_FILE" ] && grep -q "Finished processing $STATE" "$CHECKPOINT_FILE"; then
    echo "$(date): Skipping $STATE, already completed." >> "$CHECKPOINT_FILE"
    exit 0
fi

# Log start time
echo "$(date): Starting processing for $STATE" >> "$CHECKPOINT_FILE"

# Run the Python script for the current state
python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/download_discharge_monitoring_report.py "$STATE"

# Check if the script ran successfully
if [ $? -eq 0 ]; then
    echo "$(date): Finished processing $STATE" >> "$CHECKPOINT_FILE"
else
    echo "$(date): ERROR processing $STATE. Will retry." >> "$CHECKPOINT_FILE"
    exit 1  # Exit with failure to allow SLURM to retry
fi

# If job gets a timeout signal, allow requeue
trap 'echo "$(date): Job $SLURM_JOB_ID received SIGUSR1, exiting for requeue"; exit 0' SIGUSR1
