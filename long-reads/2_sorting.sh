#!/bin/bash
#SBATCH -N 4 # Ask 2 nodes
#SBATCH -n 512 # Total number of mpi jobs
#SBATCH -c 1 # use n core per mpi jobs
#SBATCH --tasks-per-node=128 # Ask n mpi jobs per node
#SBATCH --ntasks-per-socket=64
#SBATCH -t 12:0:0
#SBATCH -p thin
#SBATCH --output=sortmpi.out

module load 2021
module load impi/2021.2.0-intel-compilers-2021.2.0
#module load OpenMPI/4.1.1-GCC-10.3.0


time mpirun -n 512 /home/tahmad/tahmad/tools/mpiSORT/src/mpiSORT /scratch-shared/tahmad/bio_data/long/HG002/HG002_minimap2.sam /scratch-shared/tahmad/bio_data/long/HG002/ -q 0 -m -b
