#!/bin/bash
#SBATCH --job-name=facility_download
#SBATCH --array=0-50
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=72:00:00
#SBATCH --output=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download_logs/facility_%A_%a.out
#SBATCH --error=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download_logs/facility_%A_%a.err
#SBATCH --signal=USR1@300  # Send signal 5 minutes before timeout
#SBATCH --requeue  # Allow job to requeue on timeout

# define states
STATES=("AL" "AR" "AZ" "CA" "CO" "CT" "DC" "DE" "FL" "GA" "IA" "ID" "IL" "IN" "KS" 
"KY" "LA" "MA" "MD" "ME" "MI" "MN" "MO" "MS" "MT" "NC" "ND" "NE" "NH" "NJ" 
"NM" "NV" "NY" "OH" "OK" "OR" "PA" "RI" "SC" "SD" "TN" "TX" "UT" "VA" "VT" 
"WA" "WI" "WV" "WY")

# get the state based on SLURM array ID
STATE_INDEX=$SLURM_ARRAY_TASK_ID
STATE=${STATES[$STATE_INDEX]}

echo "$(date): Starting facility data download for $STATE"

python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download.py "$STATE"

# check if success
if [ $? -eq 0 ]; then
    echo "$(date): Python script completed successfully for $STATE"
else
    echo "$(date): ERROR processing $STATE. Check logs for details."
    exit 1
fi

# handle timeout signal
trap 'echo "$(date): Job $SLURM_JOB_ID received SIGUSR1, exiting for requeue"; exit 0' SIGUSR1