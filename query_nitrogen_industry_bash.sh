#!/bin/bash

# ==============================================================================
# Script: query_nitrogen_industry_bash.sh
#
# Description:
#   A SLURM submission script for running the Spark-based analysis of nitrogen
#   loads aggregated by industrial sector (NAICS code). It sets up the
#   environment, defines paths dynamically, and executes the
#   `query_nitrogen_industry.py` script within a container to ensure
#   a reproducible software environment.
#
#   This script is designed to be portable. It determines the project's base
#   directory from the current working directory, so it does not need to be
#   edited for different users or project locations.
#
# Usage:
#   1. Ensure your project has the required directory structure.
#   2. `cd` into your project's root directory.
#   3. Submit the script to SLURM:
#      sbatch query_nitrogen_industry_bash.sh
# ==============================================================================

# --- SLURM DIRECTIVES ---
# Note: This job requests more resources (8 CPUs, 128G memory) than previous
# jobs, as the join with the NAICS dataset can be memory-intensive.
#SBATCH --job-name=query_nitrogen_industry    # Job name for identification
#SBATCH --output=query_nitrogen_industry_logs/output_%j.log # Standard output log file
#SBATCH --error=query_nitrogen_industry_logs/error_%j.log   # Standard error log file
#SBATCH --nodes=1                             # Request a single compute node
#SBATCH --ntasks-per-node=1                   # Run a single task on the node
#SBATCH --cpus-per-task=8                     # Request 8 CPU cores for the Spark job
#SBATCH --mem=128G                            # Request 128 GB of memory
#SBATCH --time=03:00:00                       # Maximum runtime of 3 hours

# ==============================================================================
# --- SYSTEM-SPECIFIC CONFIGURATION ---
# These paths point to system-level tools. If you move this package to a new
# HPC cluster, these are likely the ONLY paths you might need to update.
CONTAINER_RUN_SCRIPT="/home/rxf131/CSE_MSE_RXF131/sdle-ondemand/pioneer/config/run.sh"
CONTAINER_IMAGE="/home/rxf131/CSE_MSE_RXF131/sdle-ondemand/build_link/apt_cpu.sif"
# ==============================================================================

# --- DYNAMIC PATH & SCRIPT CONFIGURATION ---
# Set the base directory to the current working directory (`pwd`). This makes the
# script completely portable. A user just needs to `cd` to their project root.
BASE_DIR=$(pwd)

# Define all other project-specific paths relative to the dynamic base directory.
PYTHON_SCRIPT="${BASE_DIR}/query_nitrogen_industry.py"
DMR_DATASET_PATH="${BASE_DIR}/parquet_npdes_dmrs"
NAICS_DATASET_PATH="${BASE_DIR}/parquet_npdes_naics"
OUTPUT_DIR="${BASE_DIR}/query_nitrogen_industry_reports"
VENV_PATH="${BASE_DIR}/venv/bin/activate"

# Define the target year for the analysis.
TARGET_YEAR="2023"
# Construct a descriptive output filename based on the analysis year.
OUTPUT_PATH="${OUTPUT_DIR}/query_nitrogen_industry_${TARGET_YEAR}.csv"

# --- PRE-FLIGHT CHECKS ---
echo "========================================================"
echo "Job started on $(hostname) at $(date)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Project Base Directory: ${BASE_DIR}"
echo "========================================================"

# Create log and output directories if they don't exist to prevent SLURM errors.
mkdir -p query_nitrogen_industry_logs
mkdir -p "${OUTPUT_DIR}"

# Check that necessary files and directories exist. This provides a clear,
# immediate error message if the script is run from the wrong location.
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "ERROR: Python script not found at ${PYTHON_SCRIPT}" >&2
    echo "Please ensure you run this script from your project's root directory." >&2
    exit 1
fi
if [ ! -d "${DMR_DATASET_PATH}" ]; then
    echo "ERROR: DMR data directory not found at ${DMR_DATASET_PATH}" >&2
    exit 1
fi
if [ ! -d "${NAICS_DATASET_PATH}" ]; then
    echo "ERROR: NAICS data directory not found at ${NAICS_DATASET_PATH}" >&2
    exit 1
fi

# --- MAIN EXECUTION ---
echo "Running Spark-based TOTAL NITROGEN LOAD query by industrial sector..."
echo "  Script:           ${PYTHON_SCRIPT}"
echo "  DMR Data Path:    ${DMR_DATASET_PATH}"
echo "  NAICS Data Path:  ${NAICS_DATASET_PATH}"
echo "  Year:             ${TARGET_YEAR}"
echo "  Output Path:      ${OUTPUT_PATH}"
echo "  Venv:             ${VENV_PATH}"
echo "--------------------------------------------------------"

${CONTAINER_RUN_SCRIPT} ${CONTAINER_IMAGE} /bin/bash -c "
  # Exit immediately if any command fails.
  set -e

  echo '--- Activating Python Virtual Environment inside container ---'
  source ${VENV_PATH}
  
  # This is crucial for PySpark. It tells the Spark engine to use the Python
  # interpreter from our virtual environment.
  export PYSPARK_PYTHON=\"\${VIRTUAL_ENV}/bin/python3\"
  echo \"PYSPARK_PYTHON is set to: \${PYSPARK_PYTHON}\"
  
  echo '--- Running Python script with arguments ---'
  python3 \"${PYTHON_SCRIPT}\" \"${DMR_DATASET_PATH}\" \"${NAICS_DATASET_PATH}\" \
    --year \"${TARGET_YEAR}\" \
    --output_path \"${OUTPUT_PATH}\"
  echo '--- Script finished ---'
"

# --- JOB STATUS CHECK ---
# Check the exit status of the container execution command ($?).
if [ $? -eq 0 ]; then
    echo "$(date): Script completed successfully."
else
    echo "$(date): ERROR: Script failed. Check error log: query_nitrogen_industry_logs/error_${SLURM_JOB_ID}.log"
    exit 1
fi

echo "========================================================"
echo "Job finished at $(date)"
echo "========================================================"