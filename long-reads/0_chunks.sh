#!/bin/bash
#SBATCH -N 1
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH -t 8:0:0
#SBATCH -p thin
#SBATCH --output=chunks.out

time /home/tahmad/tahmad/seqkit split2 --threads=$(nproc) -1 /scratch-shared/tahmad/bio_data/long/HG002/HG002.fastq -p 4 -O /scratch-shared/tahmad/bio_data/long/HG002/parts -f
