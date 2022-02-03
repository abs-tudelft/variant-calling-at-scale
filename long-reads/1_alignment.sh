#!/bin/bash
#SBATCH -N 4
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH -t 8:0:0
#SBATCH -p thin
#SBATCH --output=alignment.out

nodes=($(scontrol show hostname $SLURM_NODELIST))
nnodes=${#nodes[@]}
last=$(( $nnodes -1 ))
all=$(( $nnodes ))

ALIGNER="minampa2"

echo opening ssh connections to start master
ssh ${nodes[0]} hostname

i=0

echo opening ssh connections to start the other nodes worker processeses
for i in $( seq 1 $last )
do
    ssh ${nodes[i]} hostname
done
#######################################
if [ ${ALIGNER} == "minampa2" ]
then
    echo 'minampa2' 
    time /home/tahmad/tahmad/tools/minimap2/minimap2 -t $(nproc) -a -k 19  -O 5,56  -E 4,1  -B 5  -z 400,50  -r 2k  --eqx  --secondary=no -R '@RG\tID:None\tSM:sample\tPL:Pacbio\tLB:sample\tPU:lane' /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/long/HG002/parts/HG002.part_00${nnodes}.fastq > /scratch-shared/tahmad/bio_data/long/HG002/HG002_minimap2_${nnodes}.sam &
elif [ ${ALIGNER} == "winnowmap" ]
then
    echo 'winnowmap' 
    time /home/tahmad/tahmad/tools/Winnowmap/bin/winnowmap -W repetitive_k15.txt -t $(nproc) -a -k 19  -O 5,56  -E 4,1  -B 5  -z 400,50  -r 2k  --eqx  --secondary=no -R '@RG\tID:None\tSM:sample\tPL:Pacbio\tLB:sample\tPU:lane' /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/long/HG002/parts/HG002.part_00${nnodes}.fastq > /scratch-shared/tahmad/bio_data/long/HG002/HG002_winnowmap_${nnodes}.sam &
else
    echo 'LRA'
    time singularity exec /scratch-shared/tahmad/images/lra_1.3.2--h00214ad_1.sif lra align -CCS /scratch-shared/tahmad/bio_data/ref/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/long/HG002/parts/HG002.part_00${nnodes}.fastq -t $(nproc) -p s > /scratch-shared/tahmad/bio_data/long/HG002/HG002_LRA_${nnodes}.sam  &
fi
#######################################

i=0

echo starting all apps on nodes
for i in $( seq 1 $last )
do
    if [ ${ALIGNER} == "minampa2" ]
    then
	echo 'minampa2' &
        /usr/bin/ssh ${nodes[$i]} "time /home/tahmad/tahmad/tools/minimap2/minimap2 -t $(nproc) -a -k 19  -O 5,56  -E 4,1  -B 5  -z 400,50  -r 2k  --eqx  --secondary=no -R '@RG\tID:None\tSM:sample\tPL:Pacbio\tLB:sample\tPU:lane' /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/long/HG002/parts/HG002.part_00$i.fastq > /scratch-shared/tahmad/bio_data/long/HG002/HG002_minimap2_$i.sam " &
    elif [ ${ALIGNER} == "winnowmap" ]
    then 
	echo 'winnowmap' &
        /usr/bin/ssh ${nodes[$i]} " time /home/tahmad/tahmad/tools/Winnowmap/bin/winnowmap -W repetitive_k15.txt -t $(nproc) -a -k 19  -O 5,56  -E 4,1  -B 5  -z 400,50  -r 2k  --eqx  --secondary=no -R '@RG\tID:None\tSM:sample\tPL:Pacbio\tLB:sample\tPU:lane' /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/long/HG002/parts/HG002.part_00$i.fastq > /scratch-shared/tahmad/bio_data/long/HG002/HG002_winnowmap_$i.sam " &
    else
        echo 'LRA'
	/usr/bin/ssh ${nodes[$i]} " time singularity exec /scratch-shared/tahmad/images/lra_1.3.2--h00214ad_1.sif lra align -CCS /scratch-shared/tahmad/bio_data/ref/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/long/HG002/parts/HG002.part_00${i}.fastq -t $(nproc) -p s > /scratch-shared/tahmad/bio_data/long/HG002/HG002_LRA_${i}.sam " &
    fi
done

wait

sleep 5;

echo Done.
