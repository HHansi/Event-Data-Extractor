#!/bin/bash
#SBATCH --job-name=yt
#SBATCH --output=output_yt.txt
#SBATCH --error=error_yt.txt

#SBATCH --partition=cpu-48h
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=h.hettiarachchi@lancaster.ac.uk

python -u -m youtube_v2.livechat_extract -vid "pvpR2LBevPk" -folder "yt_fifa"