#!/bin/bash
#SBATCH --job-name=bs
#SBATCH --output=output_bs.txt
#SBATCH --error=error_bs.txt

#SBATCH --partition=cpu-48h
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=h.hettiarachchi@lancaster.ac.uk

python -u -m bluesky.live_extract