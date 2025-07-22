#!/bin/bash

# ==============================================================================
# download_echo_bash.sh
#
# Description:
#   This is a submission script for the SLURM workload manager. It wraps the
#   main `download_echo.sh` script to run it as a job on an HPC cluster.
#
# Usage:
#   1. Navigate to the directory containing this script.
#   2. Run: sbatch download_echo_bash.sh
#
# Note:
#   This script assumes `download_echo.sh` is in the same directory. It
#   relies on the standard SLURM behavior of starting the job in the
#   submission directory.
# ==============================================================================


# --- SLURM Directives ---

#SBATCH --job-name=download_echo_data  # Sets the name of the job.
#SBATCH --output=download_echo_logs/download_echo_%j.out # Specifies the file for standard output (%j is replaced by the job ID).
#SBATCH --error=download_echo_logs/download_echo_%j.err  # Specifies the file for standard error.
#SBATCH --time=12:00:00               # Sets the maximum wall-clock time for the job (12 hours).
#SBATCH --mem=64G                      # Requests 32 Gigabytes of memory for the job.

# --- Job Execution ---
# The '-p' flag ensures the command doesn't fail if the directory already exists.
mkdir -p "download_echo_logs"

echo "Starting the data download job."
echo "Job is running in directory: $(pwd)"

# --- Execute the Main Script ---
chmod +x ./download_echo.sh

# Execute it.
./download_echo.sh

echo "Data download job finished."