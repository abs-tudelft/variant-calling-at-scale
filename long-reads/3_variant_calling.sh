#!/bin/bash
#SBATCH -N 1
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH -t 8:0:0
#SBATCH --partition=thin

#SBATCH --output=variant_calling.out

module load 2021
module load impi/2021.2.0-intel-compilers-2021.2.0

srun -n 1 ./queue_long /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/long/HG002/ /home/tahmad/tahmad/tools/samtools_install/ 1 HG002_minimap2_sorted.bam
#srun -n 2 ./queue_long /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/ERR194147/ERR194147/output/HG002 /home/tahmad/tahmad/tools/ 24
