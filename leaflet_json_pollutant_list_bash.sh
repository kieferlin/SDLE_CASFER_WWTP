#!/bin/bash

# ==============================================================================
# Script: leaflet_json_pollutant_list_bash.sh
#
# Description:
#   A SLURM submission script that regroups the state-based JSON files (from
#   the previous step) into pollutant-based JSON files. It uses a SLURM job
#   array to process each year/label in parallel.
#
#   This script is fully portable. It derives all necessary paths from the
#   current working directory, making it reusable across different environments
#   without modification.
#
# Usage:
#   1. Run the 'leaflet_dmr_json' step first to generate the input files.
#   2. 'cd' into your project's root directory.
#   3. Submit the job to SLURM:
#      sbatch leaflet_json_pollutant_list_bash.sh
#
# ==============================================================================

# --- SLURM Directives ---
#SBATCH --job-name=leaflet_json_pollutant_list
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G # Kept high for aggregating large years like PREFY2009
#SBATCH --time=12:00:00
#SBATCH --output=leaflet_json_pollutant_list_logs/pollutant_list_%A_%a.out
#SBATCH --error=leaflet_json_pollutant_list_logs/pollutant_list_%A_%a.err
# This array processes 18 data chunks: 1 for PREFY2009 + 17 for years 2009-2025.
#SBATCH --array=0-17

# ==============================================================================
# --- DYNAMIC PATH SETUP ---
# Set the base directory to the current working directory.
BASE_DIR=$(pwd)

# Define all paths relative to the dynamic base directory.
# The input for this script is the output from the previous (leaflet_dmr_json) step.
INPUT_DIR="${BASE_DIR}/leaflet_dmr_json"
OUTPUT_DIR="${BASE_DIR}/leaflet_json_pollutant_list"
PYTHON_SCRIPT="${BASE_DIR}/leaflet_json_pollutant_list.py"

# --- PRE-FLIGHT CHECKS ---
# Create necessary log and output directories to prevent runtime errors.
mkdir -p leaflet_json_pollutant_list_logs
mkdir -p "${OUTPUT_DIR}"

# Verify that the required Python script and input directory exist.
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "ERROR: Python script not found at ${PYTHON_SCRIPT}" >&2
    echo "Please run this script from your project's root directory." >&2
    exit 1
fi
if [ ! -d "${INPUT_DIR}" ]; then
    echo "ERROR: Input directory not found at ${INPUT_DIR}" >&2
    echo "Please ensure the 'leaflet_dmr_json' directory exists and contains data." >&2
    exit 1
fi

# --- Year/Label Calculation for Job Array Task ---
# Define the explicit list of labels to process.
YEAR_LABELS=("PREFY2009" $(seq 2009 2025))

# Assign a specific label to this task using the SLURM array task ID.
LABEL_TO_PROCESS=${YEAR_LABELS[$SLURM_ARRAY_TASK_ID]}


# --- JOB EXECUTION ---
echo "========================================================"
echo "$(date): Starting pollutant grouping for LABEL=${LABEL_TO_PROCESS}"
echo "Job ID: ${SLURM_JOB_ID}, Array Task ID: ${SLURM_ARRAY_TASK_ID}"
echo "Input Dir:      ${INPUT_DIR}"
echo "Output Dir:     ${OUTPUT_DIR}"
echo "--------------------------------------------------------"

# Execute the Python script, passing all necessary paths and the label as arguments.
python3 ${PYTHON_SCRIPT} ${INPUT_DIR} ${OUTPUT_DIR} ${LABEL_TO_PROCESS}

# --- JOB STATUS CHECK ---
if [ $? -eq 0 ]; then
    echo "$(date): Pollutant grouping for LABEL=${LABEL_TO_PROCESS} completed successfully."
else
    echo "$(date): ERROR during pollutant grouping for LABEL=${LABEL_TO_PROCESS}. Check error log."
    exit 1
fi
echo "========================================================"