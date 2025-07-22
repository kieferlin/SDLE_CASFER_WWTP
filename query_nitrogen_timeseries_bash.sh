#!/bin/bash

# ==============================================================================
# Script: query_nitrogen_timeseries_bash.sh
#
# Description:
#   A SLURM submission script for running a Spark-based time-series analysis
#   of total nitrogen loads for a specified group of states. It sets up the
#   environment, defines paths dynamically, and executes the
#   `query_nitrogen_timeseries.py` script within a container.
#
#   This script is designed to be portable and parameterized. A user can easily
#   change the `STATES` variable to analyze different regions.
#
# Usage:
#   1. (Optional) Edit the `STATES` variable in this script.
#   2. `cd` into your project's root directory.
#   3. Submit the script to SLURM:
#      sbatch query_nitrogen_timeseries_bash.sh
# ==============================================================================

# --- SLURM DIRECTIVES ---
# Note: This job requests more resources (8 CPUs, 128G memory) as the
# underlying join operation can be memory-intensive.
#SBATCH --job-name=query_nitrogen_timeseries
#SBATCH --output=query_nitrogen_timeseries_logs/output_%j.log
#SBATCH --error=query_nitrogen_timeseries_logs/error_%j.log
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G
#SBATCH --time=03:00:00

# ==============================================================================
# --- SYSTEM-SPECIFIC CONFIGURATION ---
# These paths point to system-level tools. If you move this package to a new
# HPC cluster, these are likely the ONLY paths you might need to update.
CONTAINER_RUN_SCRIPT="/home/rxf131/CSE_MSE_RXF131/sdle-ondemand/pioneer/config/run.sh"
CONTAINER_IMAGE="/home/rxf131/CSE_MSE_RXF131/sdle-ondemand/build_link/apt_cpu.sif"
# ==============================================================================

# --- DYNAMIC PATH & SCRIPT CONFIGURATION ---
# Set the base directory to the current working directory (`pwd`).
BASE_DIR=$(pwd)

# Define project-specific paths relative to the dynamic base directory.
PYTHON_SCRIPT="${BASE_DIR}/query_nitrogen_timeseries.py"
DMR_DATASET_PATH="${BASE_DIR}/parquet_npdes_dmrs"
OUTPUT_DIR="${BASE_DIR}/query_nitrogen_timeseries_reports"
VENV_PATH="${BASE_DIR}/venv/bin/activate"

# --- PARAMETERS for this specific run ---
# Define the list of states to analyze. This is the main variable to change for new runs.
# This space-separated list is passed directly to the Python script's 'nargs=+' argument.
STATES="OH KY IN MI"

# --- Generate a clean filename from the parameters ---
STATES_FILENAME=$(echo "${STATES}" | tr ' ' '_')
OUTPUT_PATH="${OUTPUT_DIR}/annual_load_Nitrogen_${STATES_FILENAME}.csv"

# --- PRE-FLIGHT CHECKS ---
echo "========================================================"
echo "Job started on $(hostname) at $(date)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Project Base Directory: ${BASE_DIR}"
echo "========================================================"

# Create log and output directories if they don't exist.
mkdir -p query_nitrogen_timeseries_logs
mkdir -p "${OUTPUT_DIR}"

# Check that necessary files and directories exist.
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "ERROR: Python script not found at ${PYTHON_SCRIPT}" >&2
    exit 1
fi
if [ ! -d "${DMR_DATASET_PATH}" ]; then
    echo "ERROR: DMR data directory not found at ${DMR_DATASET_PATH}" >&2
    exit 1
fi

# --- MAIN EXECUTION ---
echo "Running Spark-based regional TOTAL NITROGEN LOAD time-series query..."
echo "  Script:           ${PYTHON_SCRIPT}"
echo "  DMR Data Path:    ${DMR_DATASET_PATH}"
echo "  States:           ${STATES}"
echo "  Output Path:      ${OUTPUT_PATH}"
echo "  Venv:             ${VENV_PATH}"
echo "--------------------------------------------------------"

${CONTAINER_RUN_SCRIPT} ${CONTAINER_IMAGE} /bin/bash -c "
  # Exit immediately if any command fails.
  set -e

  echo '--- Activating Python Virtual Environment ---'
  source ${VENV_PATH}
  
  export PYSPARK_PYTHON=\"\${VIRTUAL_ENV}/bin/python3\"
  echo \"PYSPARK_PYTHON is set to: \${PYSPARK_PYTHON}\"
  
  echo '--- Running script inside container with arguments ---'
  python3 \"${PYTHON_SCRIPT}\" \"${DMR_DATASET_PATH}\" \
    --states ${STATES} \
    --output_path \"${OUTPUT_PATH}\"
  echo '--- Script finished ---'
"

# --- JOB STATUS CHECK ---
if [ $? -eq 0 ]; then
    echo "$(date): Script completed successfully."
else
    echo "$(date): ERROR: Script failed. Check error log: query_nitrogen_timeseries_logs/error_${SLURM_JOB_ID}.log"
    exit 1
fi

echo "========================================================"
echo "Job finished at $(date)"
echo "========================================================"