#!/bin/bash

# ==============================================================================
# Script: model_oh_nitrogen_bash.sh
#
# Description:
#   A SLURM submission script for running a multi-facility nitrogen
#   forecasting pipeline. This script first identifies the top nitrogen-
#   discharging facilities in a specific state (Ohio) by load and then
#   trains a separate forecasting model for each one.
#
#   It is designed for portability, using the current working directory as the
#   project root and organizing all outputs into a unique, timestamped
#   directory for each run.
#
# Usage:
#   1. Ensure your project has the required structure (data, scripts, venv).
#   2. If needed, edit the SYSTEM-SPECIFIC container paths below.
#   3. `cd` into your project's root directory.
#   4. Submit the job to SLURM:
#      sbatch model_oh_nitrogen_bash.sh
#
# ==============================================================================

# --- SLURM Directives ---
#SBATCH --job-name=model_oh_nitrogen
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G
#SBATCH --time=03:00:00
# Log files will be saved in the 'model_oh_nitrogen_logs' directory.
# %j is replaced by the SLURM Job ID.
#SBATCH --output=model_oh_nitrogen_logs/output_%j.log
#SBATCH --error=model_oh_nitrogen_logs/error_%j.log

# ==============================================================================
# --- SYSTEM-SPECIFIC CONFIGURATION ---
# These paths point to system-level tools. If you move this project to a
# new HPC cluster, these are likely the ONLY paths you will need to update.

CONTAINER_RUN_SCRIPT="/mnt/vstor/CSE_MSE_RXF131/sdle-ondemand/pioneer/config/run.sh"
CONTAINER_IMAGE="/mnt/vstor/CSE_MSE_RXF131/sdle-ondemand/build_link/apt_gpu-tf.sif"
# ==============================================================================

# --- DYNAMIC PATH SETUP ---
# Set the base directory to the current working directory for portability.
BASE_DIR=$(pwd)

# Define all project paths relative to the dynamic BASE_DIR.
PYTHON_SCRIPT="${BASE_DIR}/model_oh_nitrogen.py"
DMR_DATA_PATH="${BASE_DIR}/parquet_npdes_dmrs"   # DMR data (time series)
PERMITS_DATA_PATH="${BASE_DIR}/parquet_icis_permits" # ICIS permits data (for flow)
PARENT_OUTPUT_DIR="${BASE_DIR}/model_oh_nitrogen_results" # Parent dir for all results
VENV_PATH="${BASE_DIR}/venv/bin/activate"

# --- PRE-FLIGHT CHECKS ---
# Create directories to prevent errors if they don't exist.
mkdir -p model_oh_nitrogen_logs
mkdir -p "${PARENT_OUTPUT_DIR}"

# Verify that key files and directories exist. This gives clear, immediate errors.
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "ERROR: Python script not found at ${PYTHON_SCRIPT}" >&2; exit 1;
fi
if [ ! -d "${DMR_DATA_PATH}" ]; then
    echo "ERROR: DMR data directory not found at ${DMR_DATA_PATH}" >&2; exit 1;
fi
if [ ! -d "${PERMITS_DATA_PATH}" ]; then
    echo "ERROR: Permits data directory not found at ${PERMITS_DATA_PATH}" >&2; exit 1;
fi
if [ ! -f "${VENV_PATH}" ]; then
    echo "ERROR: Python virtual environment not found at ${VENV_PATH}" >&2; exit 1;
fi

# --- EXPERIMENT CONFIGURATION ---
# Define the specific parameter to analyze.
TARGET_PARAMETER="Nitrogen, total [as N]"

# Create a filesystem-friendly tag from the parameter name.
PARAM_TAG=$(echo "${TARGET_PARAMETER}" | tr ' ' '_' | tr -d '[],')
# Generate a unique name for this experiment run.
EXPERIMENT_NAME="$(date +%F_%H-%M)_${PARAM_TAG}"


# --- JOB EXECUTION ---
echo "========================================================"
echo "Job started on $(hostname) at $(date)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Project Base Directory: ${BASE_DIR}"
echo "--------------------------------------------------------"
echo "Targeting Parameter: '${TARGET_PARAMETER}'"
echo "Experiment Name: ${EXPERIMENT_NAME}"
echo "Output Directory: ${PARENT_OUTPUT_DIR}/${EXPERIMENT_NAME}"
echo "========================================================"

# Execute the Python script inside a Singularity/Apptainer container.
${CONTAINER_RUN_SCRIPT} ${CONTAINER_IMAGE} /bin/bash -c "
  set -e # Exit immediately if any command fails.

  echo '--- Activating Python Virtual Environment ---'
  source ${VENV_PATH}

  echo '--- Starting Python multi-facility modeling script ---'
  python3 ${PYTHON_SCRIPT} \
    --dmr-data-path \"${DMR_DATA_PATH}\" \
    --permits-data-path \"${PERMITS_DATA_PATH}\" \
    --output-dir \"${PARENT_OUTPUT_DIR}\" \
    --run-name \"${EXPERIMENT_NAME}\" \
    --parameter-desc \"${TARGET_PARAMETER}\"

  echo '--- Python script finished ---'
"

# --- JOB STATUS CHECK ---
if [ $? -eq 0 ]; then
    echo "Container command completed successfully."
else
    echo "ERROR: Container command failed. Check error log for details."
    echo "Log file: model_oh_nitrogen_logs/error_${SLURM_JOB_ID}.log"
    exit 1
fi

echo "========================================================"
echo "Job finished at $(date)"
echo "========================================================"