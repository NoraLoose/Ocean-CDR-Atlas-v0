#!/bin/bash
#SBATCH -A m4632
#SBATCH -C cpu
#SBATCH -q regular
#SBATCH -t 48:00:00
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 32
#SBATCH -J consolidate
#SBATCH -o logs/consolidate.%j.out
#SBATCH -e logs/consolidate.%j.err

# ------------------------------
# Load environment
# ------------------------------

module load conda
conda activate romstools-test

echo "Starting job at $(date)"
echo "Running on host: $(hostname)"
echo "Using directory: $(pwd)"

# ------------------------------
# Run the script
# ------------------------------
python consolidate_forcing.py

echo "Finished job at $(date)"

