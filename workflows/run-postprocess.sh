#!/bin/bash

sbatch <<EOT
#!/bin/bash
#SBATCH -J analysis
#SBATCH -A m4746
#SBATCH -e output/postprocess/analysis-%J.out
#SBATCH -o output/postprocess/analysis-%J.out
#SBATCH --time=48:00:00
#SBATCH --mem=12GB
#SBATCH --qos=shared
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --constraint=cpu

module load python
conda activate cworthy
python -u run_postprocess.py --clobber False

EOT