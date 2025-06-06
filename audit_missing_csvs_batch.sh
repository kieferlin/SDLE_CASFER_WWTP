#!/bin/bash
#SBATCH --job-name=audit_missing_csvs
#SBATCH --output=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/logs/audit_missing_csvs.out
#SBATCH --error=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/logs/audit_missing_csvs.err
#SBATCH --time=120:00:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

# Run the audit script
python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/audit_missing_csvs.py
