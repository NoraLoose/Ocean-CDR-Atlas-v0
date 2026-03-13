#!/bin/bash
#SBATCH -A m4632
#SBATCH -C cpu
#SBATCH -q regular
#SBATCH -t 48:00:00
#SBATCH -J carbonate
#SBATCH -o logs/compute_sensitivities.%j.out
#SBATCH -e logs/compute_sensitivities.%j.err

module load conda
conda activate cworthy

echo "Running monthly configuration"
python compute_sensitivities.py --config monthly

echo "Running daily configuration"
python compute_sensitivities.py --config daily
