#!/bin/bash

# ==============================================================================
# Script: parquet_npdes_dmrs_bash.sh
#
# Description:
#   A SLURM submission script for processing the complete set of EPA ECHO DMR
#   data. It uses a SLURM job array to launch one parallel task for each year.
#   This script is designed to be portable and should not require editing paths.
#
#   It works by using the current working directory (`pwd`) as the project's
#   base directory, making it highly reusable.
#
# Usage:
#   1. Arrange your project files in the expected structure (see docs).
#   2. `cd` into your project's root directory.
#   3. Submit the script to SLURM:
#      sbatch parquet_npdes_dmrs_bash.sh
#
# ==============================================================================

# --- SLURM Directives ---
# IMPORTANT: All #SBATCH directives must come before any executable commands
# or variable assignments for SLURM to process them correctly.

#SBATCH --job-name=parquet_npdes_dmrs
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=parquet_npdes_dmrs_logs/parquet_npdes_dmrs_%A_%a.out
#SBATCH --error=parquet_npdes_dmrs_logs/parquet_npdes_dmrs_%A_%a.err
# This array range is fixed to process all years from 1983 to 2025.
#SBATCH --array=0-43

# ==============================================================================
# --- SYSTEM-SPECIFIC CONFIGURATION ---
# These paths point to system-level tools (the container runtime and image).
# If you move this package to a new HPC cluster, these are the ONLY paths
# you might need to update.

CONTAINER_RUN_SCRIPT="/home/rxf131/CSE_MSE_RXF131/sdle-ondemand/pioneer/config/run.sh"
CONTAINER_IMAGE="/home/rxf131/CSE_MSE_RXF131/sdle-ondemand/build_link/apt_cpu.sif"
# ==============================================================================

# --- DYNAMIC PATH SETUP ---
# Set the base directory to the current working directory. This makes the
# script portable. A user just needs to `cd` to their project root before
# running `sbatch`.
BASE_DIR=$(pwd)

# Define all other paths relative to this dynamic base directory.
INPUT_DIR="${BASE_DIR}/download_echo_data"
OUTPUT_DIR="${BASE_DIR}/parquet_npdes_dmrs"
PYTHON_SCRIPT="${BASE_DIR}/parquet_npdes_dmrs.py"

# --- PRE-FLIGHT CHECKS ---
# Create log directories if they don't exist to prevent SLURM errors.
mkdir -p parquet_npdes_dmrs_logs

# Check that the necessary python script and input directory exist. This provides
# a clear, immediate error message if the script is run from the wrong location.
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "ERROR: Python script not found at ${PYTHON_SCRIPT}" >&2
    echo "Please ensure you run this script from your project's root directory." >&2
    exit 1
fi
if [ ! -d "${INPUT_DIR}" ]; then
    echo "ERROR: Input data directory not found at ${INPUT_DIR}" >&2
    echo "Please ensure the 'download_echo_data' directory exists in your project root." >&2
    exit 1
fi

# --- Year Calculation for Job Array Task ---
# Calculate the specific year this task should process based on its array ID.
# The SLURM_ARRAY_TASK_ID variable is automatically set by SLURM for each task.
START_YEAR=1983
YEAR_TO_PROCESS=$((${START_YEAR} + ${SLURM_ARRAY_TASK_ID}))

# --- Job Execution ---

# Log the start of the process for this specific task.
echo "========================================================"
echo "$(date): Starting aggregation for YEAR=${YEAR_TO_PROCESS}"
echo "Job ID: ${SLURM_JOB_ID}, Array Task ID: ${SLURM_ARRAY_TASK_ID}"
echo "Base Directory: ${BASE_DIR}"
echo "Input Dir: ${INPUT_DIR}"
echo "Output Dir: ${OUTPUT_DIR}"
echo "--------------------------------------------------------"

# This variable  specifies the type of container image to use (e.g., cpu vs gpu).
imgtyp="cpu"

# Execute the main Python script.
# This command runs the Python script inside a Singularity/Apptainer container
# (`.sif` file), ensuring a consistent and reproducible software environment.
${CONTAINER_RUN_SCRIPT} \
    ${CONTAINER_IMAGE} \
    python3 ${PYTHON_SCRIPT} ${INPUT_DIR} ${OUTPUT_DIR} --start_year ${YEAR_TO_PROCESS} --end_year ${YEAR_TO_PROCESS}


# --- Job Status Check ---
# Check the exit status of the Python script that just ran.
# An exit status of 0 indicates success. Any other value indicates an error.
if [ $? -eq 0 ]; then
    echo "$(date): Aggregation for YEAR=${YEAR_TO_PROCESS} completed successfully."
else
    echo "$(date): ERROR during aggregation for YEAR=${YEAR_TO_PROCESS}. Check error log."
    exit 1 # Exit with a non-zero status to mark the SLURM task as failed.
fi
echo "========================================================"