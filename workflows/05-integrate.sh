#!/bin/bash
#SBATCH -A m4632
#SBATCH -C cpu
#SBATCH -q regular 
#SBATCH -t 48:00:00
#SBATCH -J integrate
#SBATCH -o logs/integrate.%j.out
#SBATCH -e logs/integrate.%j.err
#SBATCH --cpus-per-task=1

# Load conda module
module load conda
conda activate cworthy

SUFFIX=$1
MODE=$2

if [ -z "$SUFFIX" ]; then
    echo "Usage: sbatch extract.slurm SUFFIX"
    exit 1
fi

srun -n 1 python integrate.py $SUFFIX $MODE

