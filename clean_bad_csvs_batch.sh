#!/bin/bash
#SBATCH --job-name=clean_bad_csvs
#SBATCH --output=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/logs/clean_bad_csvs.out
#SBATCH --error=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/logs/clean_bad_csvs.err
#SBATCH --time=120:00:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

# run the cleanup script
python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/clean_bad_csvs.py
