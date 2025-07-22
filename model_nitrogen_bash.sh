#!/bin/bash

# ==============================================================================
# Script: model_nitrogen_bash.sh
#
# Description:
#   A SLURM submission script for training an XGBoost time-series forecasting
#   model. It is designed to be highly portable and reusable.
#
#   The script dynamically determines the project's base directory from its
#   current location, eliminating the need to edit project-specific paths.
#   It organizes outputs into unique, timestamped directories for each run,
#   which is a best practice for experiment tracking.
#
# Usage:
#   1. Ensure your project has the required structure (e.g., a data
#      directory, the python script, and a virtual environment).
#   2. If needed, edit the SYSTEM-SPECIFIC paths below.
#   3. `cd` into your project's root directory.
#   4. Submit the job to SLURM:
#      sbatch model_nitrogen_bash.sh
#
# ==============================================================================

# --- SLURM Directives ---
# These directives request resources from the SLURM scheduler.
#SBATCH --job-name=model_nitrogen
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G
#SBATCH --time=03:00:00
# Log files will be saved in a 'model_nitrogen_logs' directory.
# The %j placeholder is automatically replaced with the SLURM Job ID.
#SBATCH --output=model_nitrogen_logs/output_%j.log
#SBATCH --error=model_nitrogen_logs/error_%j.log

# ==============================================================================
# --- SYSTEM-SPECIFIC CONFIGURATION ---
# These paths point to system-level tools. If you move this project to a
# new HPC cluster, these are likely the ONLY paths you will need to update.

CONTAINER_RUN_SCRIPT="/mnt/vstor/CSE_MSE_RXF131/sdle-ondemand/pioneer/config/run.sh"
CONTAINER_IMAGE="/mnt/vstor/CSE_MSE_RXF131/sdle-ondemand/build_link/apt_gpu-tf.sif"
# ==============================================================================

# --- DYNAMIC PATH SETUP ---
# Set the base directory to the current working directory (`pwd`). This makes
# the script portable and ensures all relative paths are correct.
BASE_DIR=$(pwd)

# Define all project-related paths relative to the dynamic BASE_DIR.
PYTHON_SCRIPT="${BASE_DIR}/model_nitrogen.py"
DATA_PATH="${BASE_DIR}/parquet_npdes_dmrs" # Input Parquet dataset
ALL_RUNS_DIR="${BASE_DIR}/model_nitrogen_results" # Parent directory for all outputs
VENV_PATH="${BASE_DIR}/venv/bin/activate" # Path to the Python virtual environment

# --- PRE-FLIGHT CHECKS ---
# Create directories for logs and results to prevent errors.
mkdir -p model_nitrogen_logs
mkdir -p "${ALL_RUNS_DIR}"

# Verify that key files and directories exist. This provides a clear error
# if the script is not run from the project's root directory.
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "ERROR: Python script not found at ${PYTHON_SCRIPT}" >&2
    echo "Please run this script from your project's root directory." >&2
    exit 1
fi
if [ ! -d "${DATA_PATH}" ]; then
    echo "ERROR: Input data directory not found at ${DATA_PATH}" >&2
    echo "Please ensure the 'aggregate_npdes_dmrs' directory exists." >&2
    exit 1
fi
if [ ! -f "${VENV_PATH}" ]; then
    echo "ERROR: Python virtual environment not found at ${VENV_PATH}" >&2
    echo "Please ensure the 'venv' is set up correctly in the project root." >&2
    exit 1
fi

# --- EXPERIMENT CONFIGURATION ---
# Define the specific water quality parameter you want to model.
# This makes it easy to run new experiments by changing only this line.
# Example: "Nitrogen, ammonia total [as N]"
TARGET_PARAMETER="Nitrogen, total [as N]"

# Create a filesystem-friendly tag from the parameter name (e.g., "Nitrogen_total_as_N").
PARAM_TAG=$(echo "${TARGET_PARAMETER}" | tr ' ' '_' | tr -d '[],')
# Generate a unique name for this specific experiment run using the date and parameter tag.
EXPERIMENT_NAME="$(date +%F_%H-%M)_${PARAM_TAG}"


# --- JOB EXECUTION ---
echo "========================================================"
echo "Job started on $(hostname) at $(date)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Project Base Directory: ${BASE_DIR}"
echo "--------------------------------------------------------"
echo "Targeting Parameter: '${TARGET_PARAMETER}'"
echo "Experiment Name: ${EXPERIMENT_NAME}"
echo "Output Directory: ${ALL_RUNS_DIR}/${EXPERIMENT_NAME}"
echo "========================================================"

# The container run script executes the provided command inside the container environment.
# We pass a single, multi-line command enclosed in quotes to ensure it's executed correctly.
${CONTAINER_RUN_SCRIPT} ${CONTAINER_IMAGE} /bin/bash -c "
  # Exit immediately if any command fails. This is a safety measure.
  set -e

  echo '--- Activating Python Virtual Environment inside container ---'
  source ${VENV_PATH}

  echo '--- Starting Python model training script ---'
  python3 ${PYTHON_SCRIPT} \
    --data-path \"${DATA_PATH}\" \
    --output-dir \"${ALL_RUNS_DIR}\" \
    --run-name \"${EXPERIMENT_NAME}\" \
    --parameter-desc \"${TARGET_PARAMETER}\"

  echo '--- Python script finished successfully ---'
"

# --- JOB STATUS CHECK ---
# Check the exit status of the container command. A status of 0 means success.
if [ $? -eq 0 ]; then
    echo "Container command completed successfully."
else
    echo "ERROR: Container command failed. Check the error log for details."
    echo "Log file: model_nitrogen_logs/error_${SLURM_JOB_ID}.log"
    exit 1 # Exit with a non-zero status to mark the SLURM job as 'FAILED'.
fi

echo "========================================================"
echo "Job finished at $(date)"
echo "========================================================"