#!/bin/bash

# ==============================================================================
# Script: leaflet_dmr_json_bash.sh
#
# Description:
#   A SLURM submission script for processing EPA ECHO DMR data into a JSON
#   format suitable for Leaflet maps. It uses a SLURM job array to launch
#   one parallel task for each year or data label (e.g., "PREFY2009").
#
#   This script is designed to be portable and reusable. It automatically
#   sets the base directory to the current working directory, so all a user
#   needs to do is 'cd' into the project root before submitting.
#
# Usage:
#   1. Ensure your project has the required directory structure.
#   2. 'cd' into your project's root directory.
#   3. Submit the job to SLURM:
#      sbatch leaflet_dmr_json_bash.sh
#
# ==============================================================================

# --- SLURM Directives ---
# These must come before any executable commands.

#SBATCH --job-name=leaflet_dmr_json
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=12:00:00
#SBATCH --output=leaflet_dmr_json_logs/dmr_json_%A_%a.out
#SBATCH --error=leaflet_dmr_json_logs/dmr_json_%A_%a.err
# This array processes 18 distinct data chunks:
# 1 for PREFY2009 data + 17 for years 2009 through 2025.
#SBATCH --array=0-17

# ==============================================================================
# --- SYSTEM-SPECIFIC CONFIGURATION (Optional) ---
# If you run this within a containerized environment (like Singularity/Apptainer),
# define the paths to the container tools here.
# CONTAINER_RUN_SCRIPT="/path/to/run.sh"
# CONTAINER_IMAGE="/path/to/image.sif"
# ==============================================================================

# --- DYNAMIC PATH SETUP ---
# Set the base directory to the current working directory from which sbatch was called.
BASE_DIR=$(pwd)

# Define all other paths relative to the dynamic base directory.
INPUT_DIR="${BASE_DIR}/download_echo_data"
OUTPUT_DIR="${BASE_DIR}/leaflet_dmr_json"
PYTHON_SCRIPT="${BASE_DIR}/leaflet_dmr_json.py"

# --- PRE-FLIGHT CHECKS ---
# Create the log directory if it doesn't exist to prevent SLURM from failing.
mkdir -p leaflet_dmr_json_logs

# Verify that the required Python script and input data directory exist.
# This provides a clear error if the script is run from the wrong location.
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "ERROR: Python script not found at ${PYTHON_SCRIPT}" >&2
    echo "Please run this script from your project's root directory." >&2
    exit 1
fi
if [ ! -d "${INPUT_DIR}" ]; then
    echo "ERROR: Input data directory not found at ${INPUT_DIR}" >&2
    echo "Please ensure 'download_echo_data' exists in the project root." >&2
    exit 1
fi

# --- Year/Label Calculation for Job Array Task ---
# Define the explicit list of all year/labels to be processed.
# Using an array makes the mapping from task ID to label robust.
YEAR_LABELS=("PREFY2009" $(seq 2009 2025))

# Assign a specific label to this SLURM task using the array task ID as the index.
# Task 0 gets "PREFY2009", Task 1 gets "2009", and so on.
LABEL_TO_PROCESS=${YEAR_LABELS[$SLURM_ARRAY_TASK_ID]}


# --- JOB EXECUTION ---
echo "========================================================"
echo "$(date): Starting JSON generation for LABEL=${LABEL_TO_PROCESS}"
echo "Job ID: ${SLURM_JOB_ID}, Array Task ID: ${SLURM_ARRAY_TASK_ID}"
echo "Base Directory: ${BASE_DIR}"
echo "Input Dir:      ${INPUT_DIR}"
echo "Output Dir:     ${OUTPUT_DIR}"
echo "Python Script:  ${PYTHON_SCRIPT}"
echo "--------------------------------------------------------"

# Execute the Python script, passing the directories and the specific year/label
# as command-line arguments. This avoids hardcoding paths in the Python script.
python3 ${PYTHON_SCRIPT} ${INPUT_DIR} ${OUTPUT_DIR} ${LABEL_TO_PROCESS}

# --- JOB STATUS CHECK ---
# Check the exit code of the Python script. A non-zero code indicates failure.
if [ $? -eq 0 ]; then
    echo "$(date): JSON generation for LABEL=${LABEL_TO_PROCESS} completed successfully."
else
    echo "$(date): ERROR during JSON generation for LABEL=${LABEL_TO_PROCESS}. Check error log."
    exit 1 # Exit with a non-zero status to mark the SLURM task as failed.
fi
echo "========================================================"