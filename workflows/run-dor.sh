#!/bin/bash

module load python
conda activate cworthy

python run.py --cdr-forcing dor --vintage 001 --phase deploy --clobber false
