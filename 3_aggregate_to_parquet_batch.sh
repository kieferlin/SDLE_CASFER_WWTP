#!/bin/bash
#SBATCH --job-name=aggregate_dmr
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8  
#SBATCH --mem=128G           # request significant memory
#SBATCH --time=24:00:00
#SBATCH --output=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/3_aggregate_logs/agg_%A.out
#SBATCH --error=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/3_aggregate_logs/agg_%A.err

echo "$(date): Starting DMR data aggregation and conversion to Parquet"


python3 /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/3_aggregate_to_parquet.py

if [ $? -eq 0 ]; then
    echo "$(date): Aggregation script completed successfully."
else
    echo "$(date): ERROR during aggregation. Check logs."
    exit 1
fi