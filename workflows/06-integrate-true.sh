#!/bin/bash
#SBATCH -A m4632
#SBATCH -C cpu
#SBATCH -q regular 
#SBATCH -t 12:00:00
#SBATCH -J integrate_true
#SBATCH -o logs/integrate_true.%j.out
#SBATCH -e logs/integrate_true.%j.err
#SBATCH --cpus-per-task=1

# Load conda module
module load conda
conda activate cworthy

# srun -n 1 python integrate_true.py oae
srun -n 1 python integrate_true.py dor

