#!/bin/bash
#SBATCH --job-name=split_by_year_json
#SBATCH --output=2_split_by_year_logs/split_%A_%a.out
#SBATCH --error=2_split_by_year_logs/split_%A_%a.err
#SBATCH --array=0-20
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=72:00:00

# list of years
YEARS=("2007" "2008" "2009" "2010" "2011" "2012" "2013" "2014" "2015" "2016" \
       "2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025" "2026")

YEAR=${YEARS[$SLURM_ARRAY_TASK_ID]}

echo "Running for year: $YEAR"

# ensure output directory exists
mkdir -p 2_split_by_year_logs

python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/2_split_by_year.py "$YEAR"
