#!/bin/bash
#SBATCH --job-name=spark_analysis
#SBATCH --nodes=4                   
#SBATCH --cpus-per-task=16          
#SBATCH --mem=128G                 
#SBATCH --time=12:00:00
#SBATCH --output=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/4_spark_analysis_logs/spark_%A.out
#SBATCH --error=/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/4_spark_analysis_logs/spark_%A.err

# Load the Spark module if required by your HPC
# module load spark/3.x

echo "Starting Spark job submission"

# spark-submit command - how to allocate resources across the nodes SLURM gave you.
spark-submit \
  --master <your_spark_master_url> `# e.g., spark://<master-node>:<port> or yarn` \
  --num-executors 15               `# A good starting point: (nodes * cpus_per_node / cores_per_executor) - 1` \
  --executor-cores 4               `# Number of cores for each executor task` \
  --executor-memory 30G            `# Memory for each executor` \
  /home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/4_spark_analysis.py

# IMPORTANT: You MUST get the --master URL and resource allocation strategy
# from your HPC administration. Some clusters have specific scripts to launch Spark on SLURM.

echo "Spark job finished."